--1.参数列表
--1.1.优惠券id
local voucherId = ARGV[1]
--1.2.用户id
local userId = ARGV[2]
--1.3.订单id
local orderId = ARGV[3]

--2.数据key
--2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
--2.2.订单key（记录已下单的用户）
local orderKey = 'seckill:order:' .. voucherId

--3.脚本业务
--3.1.判断库存是否充足 get stockKey（key 不存在时 tonumber 返回 nil，按库存不足处理）
local stock = tonumber(redis.call('get', stockKey))
if (stock == nil or stock <= 0) then
    return 1
end

--3.2.判断用户是否已下单 SISMEMBER orderKey userId
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

--3.3.扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
--3.4.记录下单用户 sadd orderKey userId
redis.call('sadd', orderKey, userId)
--3.5.发送消息到队列 XADD seckill:stream * k1 v1 ...
redis.call('xadd', 'seckill:stream', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0
