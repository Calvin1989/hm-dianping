# 黑马点评（hm-dianping）

一个本地生活点评类后端项目，基于黑马程序员《Redis 入门到实战》课程项目完成，并在此基础上修复了异步秒杀链路中的若干问题。

> 说明：项目骨架与大部分业务代码来自课程（原作者：虎哥），本仓库记录我的学习实现与改进，改进内容见下文。

## 技术栈

Spring Boot 2.3 · Redis · Redisson · MyBatis-Plus · MySQL · Lua · Hutool

## 主要功能

- **登录**：基于 Redis 的短信验证码登录，两个拦截器分别负责登录校验和 Token 续期
- **商铺缓存**：封装 `CacheClient` 工具类，缓存空值防穿透，互斥锁 / 逻辑过期防击穿
- **优惠券秒杀**：Lua 脚本原子判断库存与一人一单，Redis Stream 消费者组异步下单，Redisson 分布式锁兜底
- **全局 ID**：基于 Redis 自增的 `RedisIdWorker`
- **社交**：点赞（ZSet）、共同关注（Set）、关注 Feed 流推送与滚动分页
- **其他**：GEO 附近商铺、BitMap 签到与连续签到统计

## 我做的改进

课程原版的异步秒杀（Redis Stream）存在以下问题，均已修复，见 `VoucherOrderServiceImpl` 与 `seckill.lua`：

1. **ACK 的 Stream 与读取的不一致**：原版从 `stream.orders` 读消息，却向 `s1` 发送 ACK，消息永远不会被确认，pending-list 持续堆积。现在 Stream 名、组名、消费者名统一为常量。
2. **启动时消费者组不存在**：原版需要手动执行 `XGROUP CREATE`，否则首次读取抛出 `NOGROUP`。现在启动时用 `XGROUP CREATE ... MKSTREAM` 自动创建，组已存在时忽略。
3. **Redis 不可用时补偿循环空转**：原版处理 pending-list 出错时无等待地重试，会占满 CPU、刷屏日志。现在加入退避，并在出错时退出补偿循环。
4. **库存 key 不存在时脚本报错**：`tonumber(nil) <= 0` 会让 Lua 脚本抛异常，现在按库存不足处理。
5. **异步下单没有事务**：原版扣数据库库存和插入订单不在同一个事务里，插入失败时库存已扣、消息未 ACK，补偿重试会再扣一次。现在用 `TransactionTemplate` 把两步放进一个事务（类内部调用 `@Transactional` 不生效），并保证先提交事务再释放锁。

## 本地运行

1. 准备 MySQL，执行 `src/main/resources/db/hmdp.sql` 建库建表
2. 准备 Redis（Stream 需要 **5.0 及以上**）
3. 按需修改 `application.yaml` 中的数据库、Redis 连接和 `hmdp.upload-dir`（图片上传目录）
4. 启动 `HmDianPingApplication`，端口 8081
5. 前端页面与 nginx 配置来自课程资料，未包含在本仓库中；放到 `src/main/resources/nginx-1.18.0/` 后启动 nginx 即可访问
6. 秒杀券需通过 `POST /voucher/seckill` 新增，接口会同时把库存写入 Redis
