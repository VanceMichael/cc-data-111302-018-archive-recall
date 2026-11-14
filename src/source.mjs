import http from 'node:http';
import {readFile} from 'node:fs/promises';

export function json(response,value,status=200){response.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});response.end(JSON.stringify(value));}

// 把已到期的 ready 任务标记为 expired，并让排队任务按 FIFO 尝试占用热存储。
// 热存储按对象 size 计费，配额不足时任务保持 queued：小任务可越过大任务先就绪。
function reconcile(state){
  for(const job of Object.values(state.jobs)){
    if(job.status==='ready'&&job.expiresAt<=state.now)job.status='expired';
  }
  let used=Object.values(state.jobs)
    .filter(job=>job.status==='ready')
    .reduce((sum,job)=>sum+job.ids.reduce((amount,id)=>amount+state.objects[id].size,0),0);
  for(const job of Object.values(state.jobs)){
    if(job.status!=='queued'||job.readyAt>state.now)continue;
    const size=job.ids.reduce((sum,id)=>sum+state.objects[id].size,0);
    if(used+size<=state.quota){
      job.status='ready';
      job.expiresAt=state.now+20;
      // 快照就绪时各资料的版本：临时副本是某一版资料的冻结拷贝
      job.versions=Object.fromEntries(job.ids.map(id=>[id,state.objects[id].version]));
      used+=size;
    }
  }
}

export async function createSource({port=0,host='127.0.0.1'}={}){
  const state=JSON.parse(await readFile(new URL('../fixtures/source.json',import.meta.url),'utf8'));

  const server=http.createServer(async(request,response)=>{
    try{
      const url=new URL(request.url,'http://local.test');let text='';
      for await(const chunk of request){text+=chunk;if(text.length>8*1024*1024){json(response,{error:'控制请求过大'},413);return;}}
      let body={};if(text){try{body=JSON.parse(text);}catch{return json(response,{error:'JSON无效'},400);}}
      if(url.pathname==='/health')return json(response,{status:'ok'});
      if(url.pathname==='/state'||url.pathname==='/inventory')return json(response,state);
      if(url.pathname==='/control'&&request.method==='POST'){
        if(body.advance!==undefined){state.now+=body.advance;reconcile(state);}
        // 测试设施：推进某卷资料的版本号，用于演示重新准备不能拼接到旧版进度
        if(body.bumpVersion!==undefined&&state.objects[body.bumpVersion]){
          state.objects[body.bumpVersion].version+=1;
        }
        if('dropResponse'in body)state.dropResponse=body.dropResponse;
        // 测试设施：只让 /recall 丢失响应（任务仍会建立），其他控制与查询端点保持可用
        if('dropRecall'in body)state.dropRecall=body.dropRecall;
        // 测试设施：/recall 在建立任务前直接拒绝，模拟请求未到达的硬失败
        if('rejectRecall'in body)state.rejectRecall=body.rejectRecall;
        // 测试设施：打开后 /data 一律 410，模拟临时副本无法领取，用于演示到期重准备
        if('blockData'in body)state.blockData=body.blockData;
        return json(response,state);
      }

      if(url.pathname==='/recall'&&request.method==='POST'){
        if(!body.requestId||!Array.isArray(body.ids)||body.ids.some(id=>!state.objects[id]))return json(response,{error:'对象无效'},400);
        if(state.rejectRecall)return json(response,{error:'召回入口不可用，请求未排队'},503);
        const prior=state.jobs[body.requestId];
        if(prior&&JSON.stringify(prior.ids)!==JSON.stringify(body.ids))return json(response,{error:'身份冲突'},409);
        // 幂等：同一 requestId + 同一 ids 重传永远返回同一个任务，调用超时不会产生重复排队
        const job=prior||{id:body.requestId,ids:body.ids,status:'queued',readyAt:state.now+10};
        state.jobs[job.id]=job;
        reconcile(state);
        if(state.dropResponse){response.socket?.destroy();return;}
        // 故障对新建与幂等重传一视同仁：任务已建但每次响应都失败，模拟持续不可用的网关
        if(state.dropRecall){
          response.writeHead(503,{'Content-Type':'application/json; charset=utf-8','Connection':'close'});
          return response.end(JSON.stringify({error:'召回网关暂不可用，任务可能已排队'}));
        }
        return json(response,job);
      }
      if(url.pathname.startsWith('/jobs/')){
        const job=state.jobs[url.pathname.split('/').pop()];
        if(!job)return json(response,{error:'不存在'},404);
        if(request.method==='DELETE'){
          job.cancelRequested=true;
          // 已占热存储的副本立即释放并退还配额；排队中的任务无法撤回，仍可能在稍后变 ready
          if(job.status==='ready'){job.status='released';job.releasedAt=state.now;}
          return json(response,job);
        }
        return json(response,job);
      }
      if(url.pathname.startsWith('/data/')){
        const job=state.jobs[url.searchParams.get('job')];
        const id=url.pathname.split('/').pop();
        if(state.blockData)return json(response,{error:'临时副本读取被故障注入阻断'},410);
        if(!job||job.status!=='ready'||job.expiresAt<=state.now||!job.ids.includes(id))return json(response,{error:'临时副本不可用'},410);
        return json(response,{id,version:job.versions[id],content:`归档${id}`});
      }

      return json(response,{error:'资源不存在'},404);
    }catch(error){if(!response.headersSent)json(response,{error:error.message},500);else response.destroy();}
  });
  await new Promise(resolve=>server.listen(port,host,resolve));
  const postControl=(body)=>new Promise((resolve,reject)=>{
    const req=http.request({host,port:server.address().port,path:'/control',method:'POST',headers:{'Content-Type':'application/json'}},res=>{res.resume();res.on('end',resolve);});
    req.on('error',reject);req.end(JSON.stringify(body));
  });
  return {
    url:`http://127.0.0.1:${server.address().port}`,state,
    advance:(amount=0,extra={})=>postControl({advance:amount,...extra}),
    bumpVersion:(id)=>postControl({bumpVersion:id}),
    blockData:(on=true)=>postControl({blockData:on}),
    dropRecall:(on=true)=>postControl({dropRecall:on}),
    close:async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));},
  };
}
