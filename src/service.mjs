import {createHash} from 'node:crypto';
import {ArchiveGateway,GatewayError} from './client.mjs';
import {Vault} from './vault.mjs';
import {needProfile,buildBatches,rankBatches,selectSubmissions,batchSize} from './planner.mjs';

const MAX_SUBMIT_ATTEMPTS=3;      // 提交超时后的幂等重试上限，超过后保持可查证状态，不无限补发
const STARVE_AFTER_TICKS=3;       // 候选被阻塞多少轮后提升优先级（防饿死）
const POLL_INTERVAL=8;            // 轮询提醒间隔（虚拟秒）
const DEFAULT_DEADLINE_TTL=12;
const TERMINAL_JOB_STATES=new Set(['done','released','expired','failed']);
const LIVE_JOB_STATES=new Set(['submitting','queued','ready']);

function needKey(requestId,itemId){return `${requestId}::${itemId}`;}

export class CollectionService {
  constructor({baseUrl,vault,clock=0}){
    this.gateway=new ArchiveGateway(baseUrl);
    this.vault=vault;
    this.now=clock;
    this.requests=new Map();    // requestId -> 请求
    this.needs=new Map();       // needKey -> 单项需求（多请求共享同一资料时指向同一在途作业）
    this.jobs=new Map();        // jobId -> 服务侧作业视图
    this.generations=new Map(); // 资料签名 -> 代次，重准备时使用新作业身份
    this.aging=new Map();       // 候选键 -> 连续被阻塞轮次
    this.events=[];
    this.metrics={mounts:0,confirmedMounts:0,hotUnitSeconds:0,fetches:0,repreps:0,submitRetries:0,quotaBlocks:0,lateReleases:0};
    this.lastPollAt=new Map();
    this.seq=0;
    this.inventory=null;
    this.quota=0;
  }

  log(kind,message,detail={}){
    this.events.push({at:this.now,seq:this.seq++,kind,message,detail});
    if(this.events.length>200)this.events.shift();
  }

  async loadInventory(){
    this.inventory=await this.gateway.inventory();
    this.quota=this.inventory.quota;
    this.log('inventory','已读取归档目录',{objects:Object.keys(this.inventory.objects),quota:this.quota});
    return this.inventory;
  }

  // 受理集合采集请求；同一资料被多个请求共享时，后续请求复用同一在途作业，不重复召回。
  // 受理只登记需求，调度在 reconcile/advance 统一进行，使同一时刻的多个请求能合并为同介质批次。
  submitRequest({requestId,ids,priority='normal',deadline}){
    if(this.requests.has(requestId))throw new Error(`请求 ${requestId} 已存在`);
    const unknown=[...new Set(ids)].filter(id=>!this.inventory.objects[id]);
    if(unknown.length)throw new Error(`未知资料：${unknown.join(',')}`);
    const dl=deadline??(this.now+DEFAULT_DEADLINE_TTL);
    const request={id:requestId,ids:[...new Set(ids)],priority,deadline:dl,state:'active',createdAt:this.now};
    this.requests.set(requestId,request);
    for(const itemId of request.ids){
      this.needs.set(needKey(requestId,itemId),{
        requestId,itemId,priority,deadline:dl,
        state:'waiting',jobId:null,savedVersion:null,bytes:0,note:null,
      });
    }
    this.log('request',`受理采集请求 ${requestId}`,{requestId,ids:request.ids,priority,deadline:dl});
    return request;
  }

  cancelRequest(requestId,{reason='用户取消'}={}){
    const request=this.requests.get(requestId);
    if(!request||request.state!=='active')return false;
    request.state='cancelled';request.cancelledAt=this.now;
    for(const need of this.#needsOf(requestId)){
      if(need.state!=='saved'&&need.state!=='cancelled'&&need.state!=='failed'){
        need.state='cancelled';need.note=reason;
      }
    }
    this.log('cancel',`请求 ${requestId} 取消，开始复核共享资料是否仍有人需要`,{requestId,reason});
    return true;
  }

  #needsOf(requestId){return [...this.needs.values()].filter(need=>need.requestId===requestId);}
  #liveNeedsForItem(itemId){
    return [...this.needs.values()].filter(need=>
      need.itemId===itemId&&!['saved','cancelled','failed'].includes(need.state));
  }

  // 推进虚拟时钟：来源是时钟权威。先在当前时刻完成调度，再推动来源，然后对新时刻完整对账，
  // 保证 t 时刻受理的请求挂载后 readyAt=t+准备时长，不会“吃掉”一次推进。
  async advance(amount=1,{dropResponse}={}){
    await this.reconcile();
    this.accountHotTime(amount);
    const state=await this.gateway.advance(amount,{dropResponse});
    this.now=state.now;
    await this.reconcile();
    return this.snapshot();
  }

  // 不推进时钟，仅对账一次（处理迟到通知、故障恢复后复查）
  async reconcile(){
    await this.#refreshInventoryVersion();
    await this.#retrySubmitting();
    await this.#observeJobs();
    await this.#collectReadyItems();
    await this.#releaseOrphanJobs();
    await this.#scheduleNewBatches();
    this.#evaluateRequests();
  }

  // 提交超时/失败的作业保持同一身份幂等重试；来源若已排队，返回的就是同一个任务
  async #retrySubmitting(){
    for(const job of this.jobs.values()){
      if(job.state!=='submitting'||job.attempts>=MAX_SUBMIT_ATTEMPTS)continue;
      job.attempts+=1;
      try{
        const result=await this.gateway.submitRecall(job.id,job.itemIds);
        job.state='queued';job.sourceStatus=result.status;job.readyAt=result.readyAt;job.note=null;
        this.metrics.confirmedMounts+=1;
        this.log('submit',`幂等重试确认作业 ${job.id} 已在来源排队（第 ${job.attempts} 次尝试）`,{jobId:job.id,attempts:job.attempts});
      }catch(error){
        this.metrics.submitRetries+=1;
        if(job.attempts>=MAX_SUBMIT_ATTEMPTS){
          job.state='failed';job.note=`提交连续 ${job.attempts} 次失败，停止补发，等待人工核查`;
          this.log('submit-failed',`作业 ${job.id} 达到重试上限，标记可查证失败`,{jobId:job.id});
          for(const id of job.itemIds){
            for(const need of this.#liveNeedsForItem(id)){need.state='failed';need.note='召回提交失败，需要人工介入';}
          }
        }else{
          job.state='submitting'; // 未确认前保持提交中，绝不在未查证时假装已排队
          job.note=`提交${error.kind==='timeout'?'超时':'失败'}，可能已在途，保持身份待幂等重试`;
          this.log('submit-retry',`作业 ${job.id} 第 ${job.attempts} 次尝试仍失败，保持同一身份`,{jobId:job.id,kind:error.kind});
        }
      }
      if(job.state==='queued'){
        for(const id of job.itemIds){
          for(const need of this.#liveNeedsForItem(id)){need.jobId=job.id;need.state='blocked';need.note=null;}
        }
      }
    }
  }

  async #refreshInventoryVersion(){
    const fresh=await this.gateway.inventory();
    for(const [id,obj] of Object.entries(fresh.objects)){
      const prev=this.inventory.objects[id];
      if(prev&&obj.version!==prev.version){
        this.log('version',`资料 ${id} 版本 ${prev.version} -> ${obj.version}，旧版字节将隔离`,{id,from:prev.version,to:obj.version});
        const moved=await this.vault.quarantineOldVersions(id,obj.version,'目录出现新版本',this.now);
        if(moved.length)this.metrics.repreps+=1;
        for(const need of this.needs.values()){
          const request=this.requests.get(need.requestId);
          const requestOpen=request&&['active','at-risk'].includes(request.state);
          if(need.itemId===id&&need.savedVersion&&need.savedVersion!==obj.version&&requestOpen){
            need.state='waiting';need.jobId=null;need.bytes=0;need.savedVersion=null;
            need.note=`旧版字节已隔离，等待新版 ${obj.version} 重新准备`;
          }
        }
        // 版本变化改变资料签名，正在途的旧版作业完成后也不会被接到新版需求上
      }
    }
    this.inventory=fresh;this.quota=fresh.quota;this.now=fresh.now;
  }

  // 逐个核查来源侧作业：通知可能迟到、取消后仍可能完成，一切以可查询的来源状态为准
  async #observeJobs(){
    for(const job of this.jobs.values()){
      if(TERMINAL_JOB_STATES.has(job.state))continue;
      const last=this.lastPollAt.get(job.id)??-Infinity;
      if(job.lastObserved!==null&&this.now-last<POLL_INTERVAL)continue;
      let observed;
      try{
        observed=await this.gateway.getJob(job.id);
        this.lastPollAt.set(job.id,this.now);
      }catch(error){
        if(error instanceof GatewayError&&error.status===404){
          // 来源查无此作业：从未确认过在途，则达到尝试上限后标记可查证失败，不自动补发
          if(job.attempts>=MAX_SUBMIT_ATTEMPTS){
            job.state='failed';
            job.note='来源查无作业且提交从未确认，需要人工核查，不自动补发';
            this.log('submit-failed',`作业 ${job.id} 来源 404 且达重试上限，标记失败待人工处理`,{jobId:job.id});
            for(const id of job.itemIds){
              for(const need of this.#liveNeedsForItem(id)){need.state='failed';need.note='召回提交失败，需要人工介入';}
            }
          }
          continue;
        }
        job.note=`状态查询${error.kind==='timeout'?'超时':'失败'}，作业仍按在途处理，不补发新任务`;
        this.log('poll',`作业 ${job.id} 状态查询${error.kind==='timeout'?'超时':'失败'}`,{jobId:job.id,kind:error.kind});
        continue;
      }
      job.lastObserved=this.now;job.sourceStatus=observed.status;job.note=null;
      if(observed.status==='ready'&&job.state!=='ready'){
        job.state='ready';job.readyAt=this.now;job.expiresAt=observed.expiresAt;job.versions=observed.versions;
        this.log('ready',`作业 ${job.id} 临时副本就绪，有效期至 t=${observed.expiresAt}${job.orphaned?'（已无存活需求，属迟到完成）':''}`,{jobId:job.id,ids:job.itemIds,expiresAt:observed.expiresAt,orphaned:!!job.orphaned});
      }else if(observed.status==='expired'){
        this.#expireJob(job,'来源显示临时副本到期');
      }else if(observed.status==='released'){
        if(job.state!=='released'){job.state='released';this.log('release',`作业 ${job.id} 已在来源侧释放`,{jobId:job.id});}
      }else if(observed.status==='queued'){
        job.state='queued';job.readyAt=observed.readyAt;
      }
    }
  }

  #expireJob(job,reason){
    if(TERMINAL_JOB_STATES.has(job.state))return;
    job.state='expired';job.note=reason;
    this.log('expire',`作业 ${job.id} ${reason}`,{jobId:job.id,ids:job.itemIds});
    // 提升资料签名代次：重新准备必须使用新作业身份，旧进度不再续接
    const next=(this.generations.get(job.signature)??job.gen)+1;
    this.generations.set(job.signature,next);
    for(const need of this.needs.values()){
      if(need.jobId===job.id&&!['saved','cancelled','failed'].includes(need.state)){
        need.jobId=null;need.state='waiting';need.note='副本到期，以新代次重新准备';
      }
    }
  }

  // 领取 ready 副本并可靠落盘；按版本核对，字节与清单原子提交
  async #collectReadyItems(){
    for(const job of this.jobs.values()){
      if(job.state!=='ready')continue;
      if(job.expiresAt<=this.now){this.#expireJob(job,'服务侧发现临时副本已到期');continue;}
      for(const itemId of job.itemIds){
        const liveNeeds=this.#liveNeedsForItem(itemId).filter(need=>need.jobId===job.id);
        const version=job.versions[itemId];
        if(await this.vault.hasVersion(itemId,version)){
          for(const need of liveNeeds)this.#markSaved(need,job,true);
          continue;
        }
        if(liveNeeds.length===0)continue; // 无人需要：等释放复核处理，不做无意义领取
        let data;
        try{
          data=await this.gateway.readData(job.id,itemId);
          this.metrics.fetches+=1;
        }catch(error){
          this.log('fetch',`资料 ${itemId} 领取失败：${error.message}`,{jobId:job.id,itemId:itemId,kind:error.kind});
          for(const need of liveNeeds)need.note=`领取失败（${error.kind}），下轮重试，副本 t=${job.expiresAt} 到期`;
          continue;
        }
        if(data.version!==version){
          // 来源版本与作业快照不符：拒绝拼接，要求重新准备，绝不把另一版接到旧进度
          this.log('version',`资料 ${itemId} 领取版本与作业快照不符，拒绝写入`,{jobId:job.id,expected:version,actual:data.version});
          for(const need of liveNeeds)need.note='版本不符，拒绝写入，等待重新准备';
          continue;
        }
        const manifest=await this.vault.save({id:itemId,version,content:data.content,savedAt:this.now,jobId:job.id});
        this.log('saved',`资料 ${itemId} v${version} 已可靠保存（${manifest.bytes} 字节，校验和 ${manifest.checksum.slice(0,10)}…）`,{jobId:job.id,itemId:itemId});
        for(const need of this.#liveNeedsForItem(itemId))this.#markSaved(need,job,false,manifest);
      }
      // 作业覆盖的需求全部解决（保存或取消）时立即释放热存储，不等 20 秒自然到期
      const stillUsing=job.itemIds.some(itemId=>
        this.#liveNeedsForItem(itemId).some(need=>need.jobId===job.id));
      if(!stillUsing){
        try{await this.gateway.cancelJob(job.id);}catch(error){this.log('release',`作业 ${job.id} 释放调用失败，等待自然到期`,{kind:error.kind});continue;}
        const wasOrphan=!!job.orphaned;
        job.state='released';job.releasedAt=this.now;
        if(wasOrphan)this.metrics.lateReleases+=1;
        this.log('release',`作业 ${job.id} 覆盖需求已全部解决，提前释放热存储`,{jobId:job.id,late:wasOrphan});
      }
    }
  }

  #markSaved(need,job,fromCache,manifest=null){
    if(need.state==='saved')return;
    need.state='saved';need.savedVersion=job.versions[need.itemId];
    need.bytes=manifest?.bytes??this.inventory.objects[need.itemId].size;
    need.note=fromCache?'命中可靠库已有版本':'临时副本已落盘并校验';
  }

  // 取消复核：作业资料已无人需要时，ready 立即释放；排队中无法撤回，标记为迟到完成候选
  async #releaseOrphanJobs(){
    for(const job of this.jobs.values()){
      if(!LIVE_JOB_STATES.has(job.state))continue;
      const wantedItems=job.itemIds.filter(itemId=>this.#liveNeedsForItem(itemId).length>0);
      if(wantedItems.length===job.itemIds.length)continue;
      if(wantedItems.length===0){
        if(job.state==='ready'){
          try{await this.gateway.cancelJob(job.id);}catch{}
          job.state='released';job.releasedAt=this.now;
          this.log('release',`共享作业 ${job.id} 取消复核：已无需求，释放热存储`,{jobId:job.id});
        }else{
          job.orphaned=true;
          job.note='全部需求已取消，来源侧排队无法撤回，迟到完成后自动释放';
          this.log('orphan',`作业 ${job.id} 已无需求但仍在途，等待迟到完成`,{jobId:job.id});
        }
      }else{
        job.orphaned=false;
        job.note=`部分资料仍被请求需要：${wantedItems.join('+')}`;
      }
    }
  }

  // 规划新批次：跳过在途作业已覆盖的资料，按公平排序在预留配额内提交
  async #scheduleNewBatches(){
    const covered=new Set();
    for(const job of this.jobs.values()){
      if(TERMINAL_JOB_STATES.has(job.state))continue;
      if(job.state==='submitting')continue; // 提交中由 #retrySubmitting 负责，避免双路径竞争
      for(const id of job.itemIds)covered.add(id);
    }
    const profiles=[];
    for(const need of this.needs.values()){
      if(['saved','cancelled'].includes(need.state))continue;
      if(covered.has(need.itemId))continue;
      if(!profiles.some(p=>p.itemId===need.itemId)){
        const profile=needProfile(need.itemId,[...this.needs.values()],this.inventory);
        if(profile)profiles.push(profile);
      }
    }
    if(profiles.length===0){this.aging.clear();return;}
    const batches=buildBatches(profiles,this.quota);
    const liveKeys=new Set(batches.map(batch=>batchKeyOf(batch)));
    for(const key of [...this.aging.keys()])if(!liveKeys.has(key))this.aging.delete(key);
    const ranked=rankBatches(batches,{now:this.now,aging:this.aging,starveAfterTicks:STARVE_AFTER_TICKS});
    const reserved=this.#reservedHotUnits();
    const {selected,blocked}=selectSubmissions(ranked,{freeQuota:this.quota-reserved});
    for(const {entry} of blocked){
      this.aging.set(entry.key,(this.aging.get(entry.key)??0)+1);
      this.metrics.quotaBlocks+=1;
      this.log('blocked',`候选批次 ${entry.key} 本轮无法提交：配额不足，已连续阻塞 ${this.aging.get(entry.key)} 轮`,{key:entry.key,freeQuota:this.quota-reserved});
    }
    for(const item of selected)this.aging.delete(item.key);
    for(const entry of selected)await this.#submitBatch(entry.batch,entry);
  }

  #reservedHotUnits(){
    let used=0;
    for(const job of this.jobs.values()){
      if(LIVE_JOB_STATES.has(job.state)){
        used+=job.itemIds.reduce((sum,id)=>sum+this.inventory.objects[id].size,0);
      }
    }
    return used;
  }

  #jobIdentity(batch){
    const signatureSource=batch.media+'|'+batch.items.map(item=>`${item.itemId}@${item.version}`).sort().join(',');
    const signature=createHash('sha1').update(signatureSource).digest('hex').slice(0,8);
    const gen=this.generations.get(signature)??0;
    return {signature,gen,jobId:`recall-${batch.media}-${signature}-g${gen}`};
  }

  async #submitBatch(batch,entry){
    const {signature,gen,jobId}=this.#jobIdentity(batch);
    const existing=this.jobs.get(jobId);
    if(existing&&existing.state!=='failed')return; // 同身份作业存活时绝不重复提交
    if(existing&&existing.attempts>=MAX_SUBMIT_ATTEMPTS)return; // 已达上限的失败作业不再自动补发
    const itemIds=batch.items.map(item=>item.itemId);
    // 失败后的同身份重建：保留累计尝试次数，使全局补发上限不被重置
    const job=existing??{
      id:jobId,media:batch.media,itemIds,state:'submitting',submittedAt:this.now,
      attempts:0,lastObserved:null,sourceStatus:null,versions:null,readyAt:null,
      expiresAt:null,releasedAt:null,note:null,signature,gen,orphaned:false,
    };
    if(existing){job.state='submitting';job.note=null;}
    this.jobs.set(jobId,job);
    job.attempts+=1;
    this.metrics.mounts+=1; // 每次提交尝试都可能已在来源挂载，先记账以便审计
    try{
      const result=await this.gateway.submitRecall(jobId,itemIds);
      job.state='queued';job.sourceStatus=result.status;job.readyAt=result.readyAt;
      this.metrics.confirmedMounts+=1;
      this.log('submit',`提交召回作业 ${jobId}（介质 ${batch.media}，资料 ${itemIds.join('+')}，体积 ${batchSize(batch)}）`,{jobId,itemIds,media:batch.media,reason:this.#why(entry)});
    }catch(error){
      // 超时可能已在来源排队：保持同一作业身份幂等重试，由尝试上限兜底，绝不换新任务号
      if(job.attempts<MAX_SUBMIT_ATTEMPTS){
        this.metrics.submitRetries+=1;
        job.note=`提交${error.kind==='timeout'?'超时':'失败'}，可能已在途，保持身份待幂等重试`;
        this.log('submit-retry',`作业 ${jobId} 提交${error.kind==='timeout'?'超时':'失败'}，保持同一身份`,{jobId,kind:error.kind,attempt:job.attempts});
      }else{
        job.state='failed';job.note=`提交连续 ${job.attempts} 次失败，停止补发，等待人工核查`;
        this.log('submit-failed',`作业 ${jobId} 达到重试上限，标记可查证失败`,{jobId});
        for(const id of itemIds){
          for(const need of this.#liveNeedsForItem(id)){need.state='failed';need.note='召回提交失败，需要人工介入';}
        }
      }
      return;
    }
    for(const id of itemIds){
      for(const need of this.#liveNeedsForItem(id)){need.jobId=jobId;need.state='blocked';need.note=null;}
    }
  }

  #why(entry){
    const parts=[`优先级档 ${entry.rank}`,`截止 t=${entry.deadline}`,`体积 ${entry.size}`];
    if(entry.waits>0)parts.push(`已等待 ${entry.waits} 轮（防饿死提权）`);
    return parts.join('，');
  }

  #evaluateRequests(){
    for(const request of this.requests.values()){
      if(!['active','at-risk'].includes(request.state))continue;
      const needs=this.#needsOf(request.id);
      if(needs.every(need=>need.state==='saved')){
        request.state='complete';request.completedAt=this.now;
        this.log('complete',`请求 ${request.id} 全部资料可靠保存完成`,{requestId:request.id});
      }else if(needs.some(need=>need.state==='failed')){
        request.state='failed';
        this.log('request-failed',`请求 ${request.id} 存在失败资料`,{requestId:request.id});
      }else if(this.now>request.deadline){
        for(const need of needs){if(!['saved','cancelled','failed'].includes(need.state))need.state='at-risk';}
        request.state='at-risk';
        this.log('risk',`请求 ${request.id} 已过截止时间仍有资料未保存`,{requestId:request.id,deadline:request.deadline});
      }
    }
  }

  // 热存储容量计费：ready 占用按推进的虚拟秒 × 体积记账
  accountHotTime(seconds){
    let units=0;
    for(const job of this.jobs.values()){
      if(job.state==='ready'&&(job.expiresAt??Infinity)>this.now){
        units+=job.itemIds.reduce((sum,id)=>sum+this.inventory.objects[id].size,0);
      }
    }
    this.metrics.hotUnitSeconds+=units*seconds;
  }

  snapshot(){
    const reserved=this.#reservedHotUnits();
    const requestViews=[...this.requests.values()].map(request=>{
      const needs=this.#needsOf(request.id);
      const saved=needs.filter(need=>need.state==='saved');
      const bytes=needs.reduce((sum,need)=>sum+(need.state==='saved'?need.bytes:0),0);
      const totalBytes=needs.reduce((sum,need)=>sum+this.inventory.objects[need.itemId].size,0);
      return {
        requestId:request.id,state:request.state,priority:request.priority,deadline:request.deadline,
        progress:`${saved.length}/${needs.length}`,bytes,totalBytes,
        completeness:needs.length?Number((saved.length/needs.length).toFixed(2)):1,
        blockers:needs.filter(need=>!['saved','cancelled'].includes(need.state)).map(need=>{
          const job=need.jobId?this.jobs.get(need.jobId):null;
          return {itemId:need.itemId,state:need.state,jobId:need.jobId,jobState:job?.state??null,note:need.note??job?.note??null};
        }),
      };
    });
    const jobViews=[...this.jobs.values()].map(job=>{
      const sharedBy=[...new Set(job.itemIds.flatMap(id=>
        [...this.needs.values()].filter(need=>need.itemId===id&&need.state==='saved').map(need=>need.requestId)))];
      return {
      jobId:job.id,media:job.media,itemIds:job.itemIds,state:job.state,
      submittedAt:job.submittedAt,readyAt:job.readyAt,expiresAt:job.expiresAt,
      size:job.itemIds.reduce((sum,id)=>sum+this.inventory.objects[id].size,0),
      sharedBy,orphaned:!!job.orphaned,
      note:job.note,attempts:job.attempts,
      };});
    return {
      now:this.now,quota:this.quota,reservedHot:reserved,freeHot:this.quota-reserved,
      metrics:{...this.metrics},requests:requestViews,jobs:jobViews,
      nextActions:this.#nextActions(),events:this.events.slice(-12),
    };
  }

  #nextActions(){
    const actions=[];
    for(const job of this.jobs.values()){
      if(job.state==='submitting')actions.push({at:this.now,actor:'scheduler',text:`幂等重试提交 ${job.id}（已尝试 ${job.attempts}/${MAX_SUBMIT_ATTEMPTS} 次）`});
      if(job.state==='ready')actions.push({at:Math.min(this.now+1,job.expiresAt),actor:'collector',text:`领取 ${job.id} 的 ${job.itemIds.join('+')}，副本 t=${job.expiresAt} 到期`});
      if(job.state==='queued')actions.push({at:job.readyAt??this.now+POLL_INTERVAL,actor:'scheduler',text:`轮询 ${job.id} 状态（来源排队中，禁止重复提交）`});
      if(job.state==='expired')actions.push({at:this.now+1,actor:'scheduler',text:`为 ${job.id} 未完成资料以新代次重新准备`});
      if(job.state==='failed')actions.push({at:null,actor:'librarian',text:`人工核查 ${job.id}，服务已停止自动补发`});
    }
    for(const [key,waits] of this.aging){
      if(waits>0)actions.push({at:this.now+1,actor:'scheduler',text:`候选 ${key} 已阻塞 ${waits} 轮，配额释放后优先提交`});
    }
    actions.sort((a,b)=>(a.at??Infinity)-(b.at??Infinity));
    return actions.slice(0,8);
  }

  renderQueue(){
    const s=this.snapshot();
    const lines=[];
    const bar=(v)=>`${'█'.repeat(Math.round(v*10))}${'░'.repeat(10-Math.round(v*10))}`;
    lines.push(`━━━ 采集队列视图 t=${s.now} ━━━`);
    lines.push(`热存储：预留 ${s.reservedHot}/${s.quota}（剩余 ${s.freeHot}）｜提交尝试 ${s.metrics.mounts} 次（确认 ${s.metrics.confirmedMounts}）｜热容量计费 ${s.metrics.hotUnitSeconds} 单位·秒｜领取 ${s.metrics.fetches} 次｜重新准备 ${s.metrics.repreps} 次｜提交重试 ${s.metrics.submitRetries} 次｜迟到释放 ${s.metrics.lateReleases} 次`);
    lines.push('');
    lines.push('采集请求：');
    if(s.requests.length===0)lines.push('  （无）');
    for(const r of s.requests){
      lines.push(`  [${r.state}] ${r.requestId} 优先级=${r.priority} 截止=t${r.deadline} 完整度=${bar(r.completeness)} ${r.progress}（${r.bytes}/${r.totalBytes} 字节）`);
      for(const b of r.blockers){
        lines.push(`      └─ ${b.itemId}: ${b.state}${b.jobId?`（作业 ${b.jobId}/${b.jobState}）`:''}${b.note?` — ${b.note}`:''}`);
      }
    }
    lines.push('');
    lines.push('召回作业：');
    if(s.jobs.length===0)lines.push('  （无）');
    for(const j of s.jobs){
      const share=j.orphaned
        ?'无存活需求（迟到完成候选）'
        :j.sharedBy.length?`共享给 ${j.sharedBy.join(',')}`:'—';
      lines.push(`  [${j.state}] ${j.jobId} 介质=${j.media} 资料=${j.itemIds.join('+')} 体积=${j.size} ${share}${j.expiresAt?` 到期=t${j.expiresAt}`:''}${j.note?` — ${j.note}`:''}`);
    }
    lines.push('');
    lines.push('预计下一动作：');
    if(s.nextActions.length===0)lines.push('  （无待办）');
    for(const a of s.nextActions)lines.push(`  t=${a.at===null?'--':a.at} [${a.actor}] ${a.text}`);
    lines.push('');
    lines.push('最近事件：');
    for(const e of s.events)lines.push(`  t=${e.at} ${e.kind}: ${e.message}`);
    return lines.join('\n');
  }
}

function batchKeyOf(batch){return `${batch.media}:${batch.items.map(item=>item.itemId).sort().join('+')}`;}
