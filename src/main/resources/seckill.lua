-- 1. 参数：userId, voucherId
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
-- 2. Key定义
local stockKey = "seckill:stock:" .. voucherId  -- 库存Key
local orderKey = "seckill:order:" .. voucherId   -- 订单Key (或购买用户集合Key)

-- 3. 业务逻辑
-- 3.1 判断库存是否充足
local stock = redis.call('get', stockKey)
if (not stock) or (tonumber(stock) <= 0) then
    return 1  -- 库存不足或已售罄
end

-- 3.2 判断用户是否已下过单 (使用SET集合实现一人一单)
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2  -- 用户已购买过
end

-- 3.3 扣减库存
redis.call('incrby', stockKey, -1)

-- 3.4 下单成功 (将用户ID添加到SET集合)
redis.call("sadd", orderKey, userId)
-- 3.5 发送消息到队列中
redis.call("xadd",'stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)

return 0 -- 秒杀成功