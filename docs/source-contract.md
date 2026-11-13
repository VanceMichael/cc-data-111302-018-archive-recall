# 来源协议

POST /recall 接受requestId与ids，同身份同内容重传。任务10个虚拟秒后可尝试准备，热存储不足保持queued；ready副本有效20秒。POST /control 的advance推进时钟，显式再次推进0也会重新尝试排队任务。GET /jobs/:id 查状态，GET /data/:id?job=领取ready副本。DELETE /jobs/:id取消请求但若已排队完成通知仍可能到达。介质字段用于业务规划，本来源按请求顺序准备，不替客户端优化。

## 通用控制

`GET /inventory` 返回本来源当前目录，`GET /health` 检查存活，`GET /state` 查看来源状态。`POST /control` 接收JSON对象，仅在来源支持的字段范围内操作。每次创建服务重新读取 fixtures/source.json；控制用于复现来源演进，不是采集业务的实现接口。

服务使用随机本地端口，测试启动与关闭均由代码负责。没有真实个人资料、凭据或付费调用。
