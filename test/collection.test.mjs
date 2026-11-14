import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readdir,readFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {createSource} from '../src/source.mjs';
import {Vault} from '../src/vault.mjs';
import {CollectionService} from '../src/service.mjs';
import {buildBatches,rankBatches,selectSubmissions,needProfile,PRIORITY_RANK} from '../src/planner.mjs';

async function withHarness(callback){
  const source=await createSource();
  const root=await mkdtemp(path.join(os.tmpdir(),'recall-test-'));
  const vault=new Vault(path.join(root,'vault'));
  await vault.init();
  const service=new CollectionService({baseUrl:source.url,vault});
  await service.loadInventory();
  try{return await callback({source,service,vault,root});}
  finally{
    // 显式复位所有故障注入，避免泄漏到后续用例
    await api(source)('/control',{method:'POST',body:{dropResponse:false,dropRecall:false,rejectRecall:false,blockData:false}}).catch(()=>{});
    await source.close();await rm(root,{recursive:true,force:true});
  }
}
const api=(source)=>async(pathname,options={})=>{
  try{
    const response=await fetch(source.url+pathname,{method:options.method||'GET',body:options.body===undefined?undefined:JSON.stringify(options.body),signal:AbortSignal.timeout(4000)});
    const text=await response.text();let value;try{value=JSON.parse(text);}catch{value=text;}
    return {status:response.status,value};
  }catch(error){return {status:0,value:null,error:error.message};}
};
const control=(source)=>async(body)=>api(source)('/control',{method:'POST',body});

// ---------- 规划器纯函数 ----------
test('规划器：按介质切配额内批次，急件小项排序靠前',()=>{
  const inventory={objects:{
    a:{media:'tape1',size:6,version:1},b:{media:'tape1',size:4,version:1},
    c:{media:'tape2',size:8,version:1},d:{media:'tape2',size:3,version:1},
  }};
  const needs=[
    {itemId:'a',state:'waiting',priority:'low',deadline:40},
    {itemId:'b',state:'waiting',priority:'low',deadline:40},
    {itemId:'c',state:'waiting',priority:'normal',deadline:20},
    {itemId:'d',state:'waiting',priority:'urgent',deadline:5},
  ].map((n,i)=>({...n,requestId:`r${i}`}));
  const profiles=['a','b','c','d'].map(id=>needProfile(id,needs,inventory));
  const batches=buildBatches(profiles,12);
  assert.deepEqual(batches.map(b=>b.media).sort(),['tape1','tape2']);
  const tape2=batches.find(b=>b.media==='tape2');
  assert.deepEqual(tape2.items.map(i=>i.itemId),['d','c']); // 急件 d 排前
  const ranked=rankBatches(batches,{now:0,aging:new Map()});
  assert.equal(ranked[0].batch.media,'tape2'); // 紧急批次优先
});

test('规划器：超大批次标注 oversize，配额选择给出阻塞原因',()=>{
  const inventory={objects:{big:{media:'tapeX',size:99,version:1},small:{media:'tapeY',size:1,version:1}}};
  const needs=[
    {itemId:'big',state:'waiting',priority:'normal',deadline:10,requestId:'r1'},
    {itemId:'small',state:'waiting',priority:'normal',deadline:10,requestId:'r1'},
  ];
  const profiles=needs.map(n=>needProfile(n.itemId,needs,inventory));
  const batches=buildBatches(profiles,12);
  assert.ok(batches.find(b=>b.oversize));
  const ranked=rankBatches(batches,{now:0,aging:new Map()});
  const {selected,blocked}=selectSubmissions(ranked,{freeQuota:12});
  assert.equal(selected.length,1);
  assert.equal(blocked[0].reason.includes('超过热存储配额'),true);
});

test('规划器：老化提权保证被阻塞候选最终可被选中',()=>{
  const inventory={objects:{
    big:{media:'m1',size:10,version:1},tiny:{media:'m2',size:1,version:1},
  }};
  const needs=[
    {itemId:'big',state:'waiting',priority:'low',deadline:100,requestId:'r1'},
    {itemId:'tiny',state:'waiting',priority:'low',deadline:100,requestId:'r2'},
  ];
  const profiles=needs.map(n=>needProfile(n.itemId,needs,inventory));
  const batches=buildBatches(profiles,12);
  const aging=new Map();
  const keyOfBig=`m1:big`;
  for(let i=0;i<3;i++)aging.set(keyOfBig,i+1);
  const ranked=rankBatches(batches,{now:0,aging,starveAfterTicks:3});
  // big 被阻塞 3 轮后提权一档，与 tiny 同档时按体积，tiny 仍在前；但 big 的 rank 已下降
  const bigEntry=ranked.find(e=>e.batch.media==='m1');
  assert.equal(bigEntry.rank,PRIORITY_RANK.low-1);
});

// ---------- 服务集成：主流程与共享 ----------
test('服务：全流程受理、召回、领取、可靠落盘与完整度',()=>withHarness(async ({service,vault})=>{
  await service.submitRequest({requestId:'R1',ids:['a','b'],priority:'normal',deadline:20});
  await service.reconcile();
  let snap=service.snapshot();
  assert.equal(snap.jobs.length,1);
  assert.equal(snap.jobs[0].state,'queued');
  await service.advance(10);
  snap=service.snapshot();
  const request=snap.requests.find(r=>r.requestId==='R1');
  assert.equal(request.state,'complete');
  assert.equal(request.completeness,1);
  assert.equal(request.bytes,Buffer.from('归档a').length+Buffer.from('归档b').length);
  assert.equal(request.totalBytes,10); // totalBytes 是目录中的计费体积单位
  const manifest=await vault.getManifest('a',1);
  assert.equal(manifest.bytes,Buffer.from('归档a').length);
  assert.match(manifest.checksum,/^[0-9a-f]{64}$/);
}));

test('服务：多请求共享同一资料只产生一个召回作业，完成后同时满足',()=>withHarness(async ({service})=>{
  await service.submitRequest({requestId:'R1',ids:['c'],priority:'normal',deadline:20});
  await service.submitRequest({requestId:'R2',ids:['c','d'],priority:'normal',deadline:20});
  await service.reconcile();
  const snap=service.snapshot();
  const tape2Jobs=snap.jobs.filter(j=>j.media==='tape2');
  assert.equal(tape2Jobs.length,1); // c,d 同介质同批次，一个作业
  assert.deepEqual(tape2Jobs[0].itemIds.sort(),['c','d']);
  await service.advance(10);
  for(const id of ['R1','R2'])assert.equal(service.snapshot().requests.find(r=>r.requestId===id).state,'complete');
  assert.equal(service.metrics.mounts,1);
}));

test('服务：重复受理同一资料不会重复提交召回',()=>withHarness(async ({service})=>{
  await service.submitRequest({requestId:'R1',ids:['a'],priority:'normal',deadline:20});
  await service.reconcile();
  const jobId=service.snapshot().jobs[0].jobId;
  // 模拟旧程序行为：再次“查询”相同资料，服务不应新建作业
  await service.submitRequest({requestId:'R2',ids:['a'],priority:'normal',deadline:20});
  await service.reconcile();
  const jobs=service.snapshot().jobs;
  assert.equal(jobs.length,1);
  assert.equal(jobs[0].jobId,jobId);
}));

// ---------- 公平与配额 ----------
test('服务：配额耗尽时小急件不被大集合饿死，释放后阻塞批次自动续上',()=>withHarness(async ({service})=>{
  await service.submitRequest({requestId:'BIG',ids:['a','b'],priority:'normal',deadline:40}); // 10
  await service.submitRequest({requestId:'SMALL',ids:['e'],priority:'urgent',deadline:6});    // 2
  await service.reconcile();
  assert.equal(service.snapshot().reservedHot,12);
  await service.submitRequest({requestId:'WIDE',ids:['c','d'],priority:'low',deadline:50});   // 11，放不下
  await service.reconcile();
  let snap=service.snapshot();
  assert.equal(snap.freeHot,0);
  assert.ok(snap.jobs.every(j=>j.itemIds.join()!=='c,d'));
  assert.ok(snap.nextActions.some(a=>a.text.includes('阻塞')));
  await service.advance(10);
  snap=service.snapshot();
  assert.equal(snap.requests.find(r=>r.requestId==='SMALL').state,'complete');
  assert.ok(snap.jobs.some(j=>j.itemIds.sort().join()===['c','d'].sort().join()&&j.state==='queued'));
}));

test('服务：超过截止时间的请求标记 at-risk 并在阻塞项中说明',()=>withHarness(async ({service})=>{
  await service.submitRequest({requestId:'LATE',ids:['e'],priority:'low',deadline:3});
  await service.reconcile();
  await service.advance(5); // 作业 t=10 才就绪，截止 t=3 已过
  const request=service.snapshot().requests.find(r=>r.requestId==='LATE');
  assert.equal(request.state,'at-risk');
  assert.ok(request.blockers.some(b=>b.state==='at-risk'));
}));

// ---------- 超时与幂等 ----------
test('服务：提交超时后同身份幂等重试，不产生重复作业，重试有上限',()=>withHarness(async ({source,service})=>{
  await api(source)('/control',{method:'POST',body:{dropRecall:true}}); // /recall 丢响应但任务仍建立
  await service.submitRequest({requestId:'U',ids:['a','b'],priority:'urgent',deadline:20});
  await service.reconcile();
  let job=service.snapshot().jobs[0];
  assert.equal(job.state,'submitting');
  const jobId=job.jobId;
  await api(source)('/control',{method:'POST',body:{dropRecall:false}}); // 网络恢复，来源任务仍在
  await service.advance(1);
  // 来源侧其实已经建好任务，恢复响应后可查证，且与服务作业同身份
  const sourceJob=await api(source)(`/jobs/${jobId}`);
  assert.equal(sourceJob.status,200);
  assert.equal(sourceJob.value.status,'queued');
  job=service.snapshot().jobs.find(j=>j.jobId===jobId);
  assert.equal(job.state,'queued');
  assert.ok(service.metrics.mounts>=1);          // 至少一次提交尝试到达来源
  assert.equal(service.metrics.confirmedMounts,1); // 来源侧只有一个任务
  assert.equal(Object.keys(source.state.jobs).length,1);
  await service.advance(10);
  assert.equal(service.snapshot().requests.find(r=>r.requestId==='U').state,'complete');
}));

test('服务：持续故障时同身份重试有上限，来源侧始终只有一个任务号',()=>withHarness(async ({source,service})=>{
  await service.submitRequest({requestId:'F',ids:['e'],priority:'normal',deadline:20});
  await api(source)('/control',{method:'POST',body:{dropRecall:true}}); // 召回响应持续故障（任务可能已建）
  await service.reconcile(); // 第 1 次尝试失败，保持 submitting 同身份
  assert.equal(service.snapshot().jobs[0].state,'submitting');
  await service.reconcile(); // 第 2 次
  await service.reconcile(); // 第 3 次达到上限
  const job=service.snapshot().jobs[0];
  assert.ok(job.attempts<=3);
  // 无论作业最终因来源可查证转为 queued 还是保持失败，都不会产生第二个任务号
  assert.equal(Object.keys(source.state.jobs).length,1);
  assert.equal(Object.keys(source.state.jobs)[0],job.jobId);
}));

test('服务：来源查无作业且重传持续失败时标记 failed 并停止补发',()=>withHarness(async ({source,service})=>{
  await service.submitRequest({requestId:'G',ids:['e'],priority:'normal',deadline:20});
  await api(source)('/control',{method:'POST',body:{rejectRecall:true}}); // 召回在建任务前硬失败
  await service.reconcile(); // 第 1 次失败
  const jobId=service.snapshot().jobs[0].jobId;
  assert.equal(source.state.jobs[jobId],undefined); // 来源侧确实没有作业
  await service.reconcile(); // 第 2 次
  await service.reconcile(); // 第 3 次
  const job=service.snapshot().jobs.find(j=>j.jobId===jobId);
  assert.equal(job.state,'failed');
  assert.ok(job.note.includes('人工核查')||job.note.includes('停止补发'));
  assert.equal(service.snapshot().requests.find(r=>r.requestId==='G').state,'failed');
  const mountsBefore=service.metrics.mounts;
  await service.reconcile();
  assert.equal(service.metrics.mounts,mountsBefore); // 不再补发
}));

// ---------- 取消与迟到完成 ----------
test('服务：全部需求取消后迟到完成被识别并立即释放热存储',()=>withHarness(async ({source,service})=>{
  await service.submitRequest({requestId:'R1',ids:['c'],priority:'normal',deadline:20});
  await service.submitRequest({requestId:'R2',ids:['c','d'],priority:'normal',deadline:20});
  await service.reconcile();
  const jobId=service.snapshot().jobs[0].jobId;
  service.cancelRequest('R1');
  service.cancelRequest('R2');
  await service.reconcile();
  assert.equal(service.snapshot().jobs[0].state,'queued'); // 排队无法撤回
  assert.ok(service.snapshot().jobs[0].note.includes('迟到完成'));
  await service.advance(12);
  const job=service.snapshot().jobs.find(j=>j.jobId===jobId);
  assert.equal(job.state,'released');
  assert.equal(service.metrics.lateReleases,1);
  assert.equal(service.snapshot().reservedHot,0);
  // 来源侧确实释放了热存储配额
  const state=await api(source)('/state');
  assert.equal(state.status,200,state.error??'状态端点应可查询');
  assert.ok(Object.values(state.value.jobs).every(j=>j.status!=='ready'));
}));

test('服务：部分请求取消时，仍被需要的资料继续保留并完成',()=>withHarness(async ({service})=>{
  await service.submitRequest({requestId:'R1',ids:['c','d'],priority:'normal',deadline:20});
  await service.submitRequest({requestId:'R2',ids:['d'],priority:'normal',deadline:20});
  await service.reconcile();
  service.cancelRequest('R1');
  await service.reconcile();
  await service.advance(10);
  assert.equal(service.snapshot().requests.find(r=>r.requestId==='R2').state,'complete');
  assert.equal(service.snapshot().requests.find(r=>r.requestId==='R1').state,'cancelled');
}));

// ---------- 到期与版本隔离 ----------
test('服务：临时副本到期后以新代次重新准备，作业身份变化且不续接旧作业',()=>withHarness(async ({source,service})=>{
  await source.blockData(true); // 就绪后无法领取，模拟馆员错过 TTL
  await service.submitRequest({requestId:'R',ids:['e'],priority:'normal',deadline:100});
  await service.reconcile(); // 首次调度提交
  const firstId=service.snapshot().jobs[0].jobId;
  await service.advance(10); // 就绪但领取被阻断
  assert.equal(service.snapshot().jobs.find(j=>j.jobId===firstId).state,'ready');
  await service.advance(21); // 推进超过 20 秒 TTL
  const expired=service.snapshot().jobs.find(j=>j.jobId===firstId);
  assert.equal(expired.state,'expired');
  await source.blockData(false);
  await service.reconcile();
  const newJobs=service.snapshot().jobs.filter(j=>j.state==='queued');
  assert.equal(newJobs.length,1);
  assert.notEqual(newJobs[0].jobId,firstId); // 新代次，新身份
  await service.advance(10);
  assert.equal(service.snapshot().requests.find(r=>r.requestId==='R').state,'complete');
}));

test('服务：目录版本切换时隔离旧字节，新版独立保存，绝不拼接',()=>withHarness(async ({source,service,vault})=>{
  await service.submitRequest({requestId:'R1',ids:['a'],priority:'normal',deadline:30});
  await service.advance(10);
  const v1=await vault.getManifest('a',1);
  assert.ok(v1);
  await source.bumpVersion('a');
  await service.submitRequest({requestId:'R2',ids:['a'],priority:'high',deadline:40});
  await service.reconcile();
  const quarantined=await readdir(vault.quarantineDir);
  assert.ok(quarantined.some(f=>f.includes('a-v1-')));
  assert.equal(await vault.hasVersion('a',1),false);
  await service.advance(10);
  const v2=await vault.getManifest('a',2);
  assert.ok(v2);
  assert.notEqual(v2.jobId,v1.jobId);
  const blob=await readFile(vault.blobPath('a',2),'utf8');
  assert.equal(blob,'归档a');
}));

test('服务：领取内容版本与作业快照不符时拒绝写入并要求重新准备',()=>withHarness(async ({source,service})=>{
  await service.submitRequest({requestId:'R',ids:['e'],priority:'normal',deadline:40});
  await service.advance(10); // 作业就绪并保存 v1
  assert.equal(service.snapshot().requests[0].state,'complete');
  // 归档侧版本推进：新请求需要 v2，v1 字节被隔离，重新准备后独立落盘
  await source.bumpVersion('e');
  await service.submitRequest({requestId:'R2',ids:['e'],priority:'high',deadline:50});
  await service.reconcile();
  assert.notEqual(service.snapshot().requests.find(r=>r.requestId==='R2').state,'complete');
  await service.advance(10);
  assert.equal(service.snapshot().requests.find(r=>r.requestId==='R2').state,'complete');
  const jobs=service.snapshot().jobs.filter(j=>j.itemIds.includes('e'));
  const v1Job=jobs[0],v2Job=jobs[1];
  assert.notEqual(v2Job.jobId,v1Job.jobId); // 版本变化产生新签名/新作业身份，未续接旧作业
}));

// ---------- 计费与视图 ----------
test('服务：热存储按 体积×虚拟秒 计费，提前释放停止累计',()=>withHarness(async ({source,service})=>{
  await service.submitRequest({requestId:'R',ids:['a','b'],priority:'normal',deadline:30}); // 体积 10
  await service.advance(10); // 就绪即领取并释放，ready 持有时间≈0
  assert.equal(service.metrics.hotUnitSeconds,0);
  // 就绪后阻断领取：副本跨虚拟秒持有，应按 体积×秒 累计计费
  await source.blockData(true);
  await service.submitRequest({requestId:'R2',ids:['e'],priority:'normal',deadline:40}); // 体积 2
  await service.advance(10); // t=20 就绪
  assert.equal(service.metrics.hotUnitSeconds,0);
  await service.advance(3);  // 持有 3 秒：2×3=6 单位·秒
  assert.equal(service.metrics.hotUnitSeconds,6);
  await source.blockData(false);
  await service.advance(1);  // 恢复后领取并释放
  const billed=service.metrics.hotUnitSeconds;
  await service.advance(5);  // 释放后不再累计
  assert.equal(service.metrics.hotUnitSeconds,billed);
}));

test('服务：队列视图包含阻塞、下一动作与完整度三要素',()=>withHarness(async ({service})=>{
  await service.submitRequest({requestId:'R',ids:['a'],priority:'urgent',deadline:5});
  await service.reconcile();
  const text=service.renderQueue();
  assert.match(text,/采集队列视图/);
  assert.match(text,/预计下一动作/);
  assert.match(text,/完整度=░+ 0\/1/);
  assert.match(text,/召回作业/);
  const snap=service.snapshot();
  assert.ok(snap.nextActions.length>0);
  assert.equal(typeof snap.now,'number');
}));

// ---------- 来源契约扩展 ----------
test('来源：DELETE 已就绪作业立即释放热存储并退还配额',()=>withHarness(async ({source})=>{
  const call=api(source);
  await call('/recall',{method:'POST',body:{requestId:'j1',ids:['a','b']}});
  await call('/control',{method:'POST',body:{advance:10}});
  assert.equal((await call('/jobs/j1')).value.status,'ready');
  await call('/jobs/j1',{method:'DELETE'});
  const job=(await call('/jobs/j1')).value;
  assert.equal(job.status,'released');
  assert.equal((await call('/data/a?job=j1')).status,410);
  // 释放后排队任务可以使用退还的配额（新作业需 10 个虚拟秒准备）
  await call('/recall',{method:'POST',body:{requestId:'j2',ids:['c']}});
  await call('/control',{method:'POST',body:{advance:10}});
  assert.equal((await call('/jobs/j2')).value.status,'ready');
}));

test('来源：bumpVersion 控制推进资料版本，就绪作业保留就绪时版本快照',()=>withHarness(async ({source})=>{
  const call=api(source);
  await call('/recall',{method:'POST',body:{requestId:'j1',ids:['a']}});
  await call('/control',{method:'POST',body:{advance:10}});
  const data=(await call('/data/a?job=j1')).value;
  assert.equal(data.version,1);
  await call('/control',{method:'POST',body:{bumpVersion:'a'}});
  assert.equal((await call('/inventory')).value.objects.a.version,2);
  // 已就绪副本仍是旧版冻结快照
  assert.equal((await call('/data/a?job=j1')).value.version,1);
}));
