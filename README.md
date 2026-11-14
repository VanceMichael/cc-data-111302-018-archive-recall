# 冷归档资料召回的介质配额与迟到领取

这是用于内部数据接入的 Node.js 项目。当前提供可独立启动的本地来源、单次原文读取接口以及来源契约测试；浏览器页面和仪器网关使用合成业务资料，不连接真实账户或设备。

## 环境与命令

Node.js 22.22.1 或更新的 22.x。依赖由 package-lock.json 锁定。

```sh
npm ci --ignore-scripts
npm run build
npm test
npm start
```

`npm start` 在标准输出报告来源地址。默认绑定本机回环地址并使用动态端口；可通过 `HOST` 与 `PORT` 指定监听地址和端口，例如 `HOST=127.0.0.1 PORT=8080 npm start`。按 Ctrl+C 关闭来源；动态端口避免与同机其他接入站冲突。`npm run demo` 在同一进程启动来源，读取一次目录后退出，不代表持续接收业务已经完成。`npm run demo:collection` 启动面向资料集合的召回采集演示，用可推进时钟复现配额耗尽、急件插入、超时在途、全部取消后的迟到完成与版本隔离，不做真实冷存储等待；可加场景参数 `npm run demo:collection -- quota|timeout|cancel|version`。

```sh
docker build -t archive-recall:local .
docker run --rm --network none archive-recall:local
docker run --rm --network none archive-recall:local npm run demo
```

依赖在镜像构建阶段安装，运行阶段不需要公网。完整来源测试和一次目录演示通常数秒即可结束，适合在90秒以内查看真实输出。浏览器在无界面模式执行，页面交互不依赖人工鼠标。

## 目录

- `src/source.mjs`：本地来源生命周期与HTTP入口。
- `src/client.mjs`：目录读取、原始字节获取与区分超时/故障的采集网关客户端。
- `src/planner.mjs`：纯函数召回规划（介质批次、公平排序、老化提权、阻塞原因）。
- `src/service.mjs`：集合召回采集服务（共享去重、幂等重试、取消复核、到期/版本处理、队列视图）。
- `src/vault.mjs`：按版本隔离的可靠保存库（原子写入、校验和清单、隔离区）。
- `src/demo-collection.mjs`：可推进时钟的采集服务演示。
- `fixtures/source.json`：来源初始状态。
- `docs/source-contract.md`：来源协议范围、控制与领域语义。
- `docs/collection-service.md`：采集服务设计决策与场景说明。
- `test/source.test.mjs`：实际来源集成测试。
- `test/collection.test.mjs`：规划器与采集服务集成测试。

运行时状态仅用于来源模拟，重启来源会回到 fixtures 中的值。接入业务自己的持久状态请使用 `.local/` 或明确的外部路径。来源管理入口 `/state` 和 `/control` 是本地操作设施，不能作为绕过浏览器页面的业务写入捷径。
