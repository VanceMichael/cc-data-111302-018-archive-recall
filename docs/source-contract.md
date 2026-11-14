# 来源协议

POST /recall 接受requestId与ids，同身份同内容重传。任务10个虚拟秒后可尝试准备，热存储不足保持queued；ready副本有效20秒。POST /control 的advance推进时钟，显式再次推进0也会重新尝试排队任务。GET /jobs/:id 查状态，GET /data/:id?job=领取ready副本。DELETE /jobs/:id取消请求：排队任务仅登记取消（完成通知仍可能到达），ready副本立即释放并退还热存储配额。介质字段用于业务规划，本来源按请求顺序准备，不替客户端优化。

## 测试故障注入（仅本地模拟）

POST /control 额外接受：dropResponse（丢弃全部HTTP响应）、dropRecall（/recall 建任务后返回503，模拟“可能已排队”的不确定响应，对幂等重传同样生效）、rejectRecall（/recall 建任务前直接503，模拟请求未到达）、blockData（/data 一律410，模拟副本无法领取）、bumpVersion（推进指定资料版本号，已就绪副本保留就绪时的版本快照）。这些字段只用于复现演进与故障，不是采集业务的正常接口。

## 通用控制

`GET /inventory` 返回本来源当前目录，`GET /health` 检查存活，`GET /state` 查看来源状态。`POST /control` 接收JSON对象，仅在来源支持的字段范围内操作。每次创建服务重新读取 fixtures/source.json；控制用于复现来源演进，不是采集业务的实现接口。

服务使用随机本地端口，测试启动与关闭均由代码负责。没有真实个人资料、凭据或付费调用。
