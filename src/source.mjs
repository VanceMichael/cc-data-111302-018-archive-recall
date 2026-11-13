import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';

export function json(response,value,status=200){response.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});response.end(JSON.stringify(value));}
export async function createSource({port=0,host='127.0.0.1'}={}){
  const state=JSON.parse(await readFile(new URL('../fixtures/source.json',import.meta.url),'utf8'));
  
  const pages={};
  const server=http.createServer(async(request,response)=>{
    try{
      const url=new URL(request.url,'http://local.test');let text='';
      for await(const chunk of request){text+=chunk;if(text.length>8*1024*1024){json(response,{error:'控制请求过大'},413);return;}}
      let body={};if(text){try{body=JSON.parse(text);}catch{return json(response,{error:'JSON无效'},400);}}
      if(url.pathname==='/health')return json(response,{status:'ok'});
      if(url.pathname==='/state'||url.pathname==='/inventory')return json(response,state);
      if(url.pathname==='/control'&&request.method==='POST'){if(body.advance!==undefined){state.now+=body.advance;for(const job of Object.values(state.jobs))if(job.status==='ready'&&job.expiresAt<=state.now)job.status='expired';let used=Object.values(state.jobs).filter(job=>job.status==='ready').reduce((sum,job)=>sum+job.ids.reduce((amount,id)=>amount+state.objects[id].size,0),0);for(const job of Object.values(state.jobs)){const size=job.ids.reduce((sum,id)=>sum+state.objects[id].size,0);if(job.status==='queued'&&job.readyAt<=state.now&&used+size<=state.quota){job.status='ready';job.expiresAt=state.now+20;job.versions=Object.fromEntries(job.ids.map(id=>[id,state.objects[id].version]));used+=size;}}}if('dropResponse'in body)state.dropResponse=body.dropResponse; return json(response,state);}
      if(Object.hasOwn(pages,url.pathname)){response.setHeader('Content-Type','text/html; charset=utf-8');return response.end(await readFile(new URL('../public/'+pages[url.pathname],import.meta.url)));}
      
    if(url.pathname==='/recall'&&request.method==='POST'){
      if(!body.requestId||!Array.isArray(body.ids)||body.ids.some(id=>!state.objects[id]))return json(response,{error:'对象无效'},400);
      const prior=state.jobs[body.requestId];if(prior&&JSON.stringify(prior.ids)!==JSON.stringify(body.ids))return json(response,{error:'身份冲突'},409);
      const job=prior||{id:body.requestId,ids:body.ids,status:'queued',readyAt:state.now+10};state.jobs[job.id]=job;if(state.dropResponse)return response.destroy();return json(response,job);
    }
    if(url.pathname.startsWith('/jobs/')){const job=state.jobs[url.pathname.split('/').pop()];if(!job)return json(response,{error:'不存在'},404);if(request.method==='DELETE')job.cancelRequested=true;return json(response,job);}
    if(url.pathname.startsWith('/data/')){const job=state.jobs[url.searchParams.get('job')];const id=url.pathname.split('/').pop();if(!job||job.status!=='ready'||job.expiresAt<=state.now||!job.ids.includes(id))return json(response,{error:'临时副本不可用'},410);return json(response,{id,version:job.versions[id],content:`归档${id}`});}

      return json(response,{error:'资源不存在'},404);
    }catch(error){if(!response.headersSent)json(response,{error:error.message},500);else response.destroy();}
  });
  await new Promise(resolve=>server.listen(port,host,resolve));
  return {url:`http://127.0.0.1:${server.address().port}`,state,close:async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}};
}
