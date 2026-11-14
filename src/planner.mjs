// 纯函数规划层：把“哪些资料还缺、各自由谁需要”变成带排序理由的提交候选批次。
// 不接触网络与文件，方便单独测试。

export const PRIORITY_RANK={urgent:0,high:1,normal:2,low:3};

// 单项资料的“需求画像”：所有仍未终结的请求里最急的优先级、最早的截止时间。
export function needProfile(itemId,needs,inventory){
  const waiting=needs.filter(need=>need.itemId===itemId&&!['saved','cancelled'].includes(need.state));
  if(waiting.length===0)return null;
  return {
    itemId,
    media:inventory.objects[itemId].media,
    size:inventory.objects[itemId].size,
    version:inventory.objects[itemId].version,
    bestRank:Math.min(...waiting.map(need=>PRIORITY_RANK[need.priority]??PRIORITY_RANK.normal)),
    earliestDeadline:Math.min(...waiting.map(need=>need.deadline)),
    requestIds:waiting.map(need=>need.requestId),
  };
}

// 按介质把缺失资料切成不超过配额的批次（一次挂载尽量多带几项，单卷可一次准备多项）。
// 介质内先按急件/截止时间/体积排序，使小而急的资料优先进入靠前批次。
export function buildBatches(profiles,quota){
  const byMedia=new Map();
  for(const profile of profiles){
    if(!byMedia.has(profile.media))byMedia.set(profile.media,[]);
    byMedia.get(profile.media).push(profile);
  }
  const batches=[];
  for(const [media,items] of byMedia){
    items.sort((a,b)=>
      (a.bestRank-b.bestRank)||
      (a.earliestDeadline-b.earliestDeadline)||
      (a.size-b.size)||
      a.itemId.localeCompare(b.itemId));
    let current=[];let used=0;
    for(const item of items){
      if(item.size>quota){batches.push({media,items:[item],oversize:true});continue;}
      if(used+item.size>quota&&current.length>0){batches.push({media,items:current,oversize:false});current=[];used=0;}
      current.push(item);used+=item.size;
    }
    if(current.length>0)batches.push({media,items:current,oversize:false});
  }
  return batches;
}

// 公平排序：优先级 -> 截止时间 -> 批次体积（小请求不被大集合饿死）-> 首次出现次序。
// aging 为每个候选键记录被阻塞的轮次，超过阈值提升一档优先级，保证任何候选最终能被选中。
export function rankBatches(batches,{now,aging,starveAfterTicks=3}={}){
  const decorated=batches.map((batch,index)=>{
    const key=batchKey(batch);
    const waits=aging.get(key)??0;
    const rank=Math.max(0,Math.min(...batch.items.map(item=>item.bestRank))-Math.floor(waits/starveAfterTicks));
    const deadline=Math.min(...batch.items.map(item=>item.earliestDeadline));
    const size=batch.items.reduce((sum,item)=>sum+item.size,0);
    return {batch,key,index,rank,deadline,size,waits};
  });
  decorated.sort((a,b)=>
    (a.rank-b.rank)||
    (a.deadline-b.deadline)||
    (a.size-b.size)||
    (a.index-b.index));
  return decorated;
}

export function batchKey(batch){return `${batch.media}:${batch.items.map(item=>item.itemId).sort().join('+')}`;}

export function batchSize(batch){return batch.items.reduce((sum,item)=>sum+item.size,0);}

// 选择本轮能在预留配额内提交的候选；返回选中项与未选中原因，供队列视图解释阻塞。
export function selectSubmissions(ranked,{freeQuota}={}){
  const selected=[];const blocked=[];
  let remaining=freeQuota;
  for(const entry of ranked){
    const size=batchSize(entry.batch);
    if(entry.batch.oversize){blocked.push({entry,reason:'单项体积超过热存储配额，需要馆员扩容'});continue;}
    if(size<=remaining){selected.push(entry);remaining-=size;}
    else blocked.push({entry,reason:`热存储配额不足（本批 ${size}，剩余 ${remaining}）`});
  }
  return {selected,blocked,remaining};
}
