// 本地冷归档来源的 HTTP 客户端。所有调用都由服务的虚拟时钟驱动，不做真实等待。
export class GatewayError extends Error {
  constructor(message,{status,kind}={}){super(message);this.name='GatewayError';this.status=status;this.kind=kind;}
}

async function call(url,options={}){
  let response;
  try{
    response=await fetch(url,{...options,signal:AbortSignal.timeout(options.timeoutMs??2000)});
  }catch(error){
    // 网络错误/超时：请求可能已经到达来源并排队，必须按“在途未知”处理，禁止盲目新建任务
    throw new GatewayError(`来源调用失败：${error.message}`,{kind:error.name==='TimeoutError'||error.name==='AbortError'?'timeout':'network'});
  }
  const text=await response.text();
  let value;try{value=text?JSON.parse(text):{};}catch{value={};}
  if(!response.ok)throw new GatewayError(`来源返回 ${response.status}`,{status:response.status,kind:'http'});
  return value;
}

export class ArchiveGateway {
  constructor(baseUrl){this.baseUrl=baseUrl;}
  inventory(){return call(`${this.baseUrl}/inventory`);}
  submitRecall(requestId,ids){return call(`${this.baseUrl}/recall`,{method:'POST',body:JSON.stringify({requestId,ids})});}
  getJob(jobId){return call(`${this.baseUrl}/jobs/${encodeURIComponent(jobId)}`);}
  cancelJob(jobId){return call(`${this.baseUrl}/jobs/${encodeURIComponent(jobId)}`,{method:'DELETE'});}
  readData(jobId,id){return call(`${this.baseUrl}/data/${encodeURIComponent(id)}?job=${encodeURIComponent(jobId)}`);}
  // 推进来源虚拟时钟；显式推进 0 也会让来源重新尝试排队任务
  advance(amount,{dropResponse}={}){
    return call(`${this.baseUrl}/control`,{method:'POST',body:JSON.stringify({advance:amount,dropResponse})});
  }
  bumpVersion(id){return call(`${this.baseUrl}/control`,{method:'POST',body:JSON.stringify({bumpVersion:id})});}
}

export async function inspect(url){
  const response=await fetch(url+'/inventory',{signal:AbortSignal.timeout(2000)});
  if(!response.ok)throw new Error(`目录读取失败: ${response.status}`);
  return response.json();
}
export async function readRaw(url,path){
  const response=await fetch(url+path,{signal:AbortSignal.timeout(2000)});
  if(!response.ok)throw new Error(`资料读取失败: ${response.status}`);
  return Buffer.from(await response.arrayBuffer());
}
