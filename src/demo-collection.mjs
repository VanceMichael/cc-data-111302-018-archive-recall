import {mkdtemp,rm,readdir} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {createSource} from './source.mjs';
import {Vault} from './vault.mjs';
import {CollectionService} from './service.mjs';

// 演示全部由虚拟时钟驱动：advance 一次只推来源时钟并触发服务对账，不做真实冷存储等待。
async function withHarness(callback){
  const source=await createSource();
  const root=await mkdtemp(path.join(os.tmpdir(),'recall-demo-'));
  const vault=new Vault(path.join(root,'vault'));
  await vault.init();
  const service=new CollectionService({baseUrl:source.url,vault});
  await service.loadInventory();
  try{return await callback({source,service,vault,root});}
  finally{await source.close();await rm(root,{recursive:true,force:true});}
}

function banner(title){console.log('\n'+'='.repeat(72));console.log('场景：'+title);console.log('='.repeat(72));}
function tick(label){console.log(`\n----- ${label} -----`);}

async function scenarioQuotaFairness(){
  banner('配额耗尽：大集合不饿死小请求，小急件插队先完成');
  await withHarness(async ({service})=>{
    // 大集合 a(6)+b(4)=10，几乎用尽配额 12
    service.submitRequest({requestId:'R-BIG',ids:['a','b'],priority:'normal',deadline:30});
    await service.reconcile();
    // 小请求只需要 e(2)，带更早截止时间：按 优先级/截止/体积 排序可与大集合同轮提交
    service.submitRequest({requestId:'R-SMALL',ids:['e'],priority:'urgent',deadline:6});
    await service.reconcile();
    tick('t=0 受理两个请求：两个批次都在配额内，小急件排序在前，各自只挂载一次');
    console.log(service.renderQueue());

    // 再来一个大集合 c(8)+d(3)=11，与在途作业叠加后超出配额
    service.submitRequest({requestId:'R-WIDE',ids:['c','d'],priority:'low',deadline:40});
    await service.reconcile();
    tick('t=0 追加低优先级大集合：热存储预留已满，新批次进入阻塞并给出原因与等待轮次');
    console.log(service.renderQueue());

    await service.advance(10);
    tick('t=10 推进 10 个虚拟秒：在途副本就绪并立即领取，可靠库落盘后小急件先完成');
    console.log(service.renderQueue());

    await service.advance(1);
    tick('t=11 完成的作业提前释放热存储，被阻塞的大集合自动获得配额并提交，无需人工干预');
    console.log(service.renderQueue());
  });
}

async function scenarioTimeoutInFlight(){
  banner('调用超时可能已经排队：幂等重试 + 状态可查证，不无限补发');
  await withHarness(async ({source,service})=>{
    // 来源开启召回故障：POST /recall 已在来源侧建任务，但每次响应都是 503
    await source.dropRecall(true);
    service.submitRequest({requestId:'R-URGENT',ids:['a','b'],priority:'urgent',deadline:15});
    await service.reconcile();
    tick('t=0 提交响应故障：服务标记“可能已在途”，保留同一作业身份，不新建任务号');
    console.log(service.renderQueue());

    await source.dropRecall(false); // 网络恢复，来源任务仍在
    await service.advance(1);
    tick('t=1 幂等重试命中同一 requestId，来源返回同一个任务：确认挂载数不重复增长');
    console.log(service.renderQueue());

    await service.advance(10);
    tick('t=11 查询到作业就绪并完成领取：全程来源侧只有一个召回任务');
    console.log(service.renderQueue());
  });
}

async function scenarioCancelLateCompletion(){
  banner('共享请求全部取消：迟到完成可查证，就绪即释放，不白占热存储');
  await withHarness(async ({service})=>{
    service.submitRequest({requestId:'R-1',ids:['c'],priority:'normal',deadline:20});
    service.submitRequest({requestId:'R-2',ids:['c','d'],priority:'normal',deadline:20});
    await service.reconcile();
    tick('t=0 两个请求共享资料 c，只产生一个召回作业');
    console.log(service.renderQueue());

    service.cancelRequest('R-1');
    service.cancelRequest('R-2');
    await service.reconcile();
    tick('t=0 全部需求取消：排队作业无法撤回，标记为“迟到完成候选”，不新建任务');
    console.log(service.renderQueue());

    await service.advance(12);
    tick('t=12 迟到完成到达：服务查到 ready 后无人领取，立即释放热存储并退还配额');
    console.log(service.renderQueue());
  });
}

async function scenarioVersionIsolation(){
  banner('临时副本到期/版本切换：重新准备不拼接旧版字节，完整度可查证');
  await withHarness(async ({source,service,vault})=>{
    service.submitRequest({requestId:'R-V',ids:['a'],priority:'normal',deadline:30});
    await service.advance(10);
    tick('t=10 资料 a v1 已可靠保存，完整度 100%');
    console.log(service.renderQueue());

    // 归档侧把 a 换成新版本（模拟重新整理后版本号推进）
    await source.bumpVersion('a');
    service.submitRequest({requestId:'R-V2',ids:['a'],priority:'high',deadline:25});
    await service.reconcile();
    tick('t=10 目录出现 a v2：v1 字节移入隔离区，新需求以新作业身份重新准备');
    const quarantined=await readdir(vault.quarantineDir);
    console.log(`隔离区内容：${quarantined.join(', ')||'(空)'}`);
    console.log(service.renderQueue());

    await service.advance(10);
    tick('t=20 新版 v2 独立落盘：清单分别记录版本/字节/校验和，两份资料互不续写');
    const saved=await vault.listSaved();
    console.log('可靠库当前版本清单（旧版在隔离区，不在此列）：');
    for(const manifest of saved)console.log(`  ${manifest.id} v${manifest.version}：${manifest.bytes} 字节 sha256=${manifest.checksum.slice(0,16)}… 作业=${manifest.jobId}`);
    console.log('\n'+service.renderQueue());
  });
}

const scenarios={quota:scenarioQuotaFairness,timeout:scenarioTimeoutInFlight,cancel:scenarioCancelLateCompletion,version:scenarioVersionIsolation};
const picked=process.argv[2]?[process.argv[2]]:Object.keys(scenarios);
for(const name of picked){
  if(!scenarios[name])throw new Error(`未知场景：${name}，可选 ${Object.keys(scenarios).join('/')}`);
  await scenarios[name]();
}
console.log('\n全部演示结束：未使用任何真实等待，所有状态均可通过服务快照与来源 /jobs/:id 查证。');
