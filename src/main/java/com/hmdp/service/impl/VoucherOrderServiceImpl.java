package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 优惠券秒杀下单
 * </p>
 * 流程：Lua 脚本在 Redis 中判断库存与一人一单并写入 Stream，
 * 后台线程消费 Stream，在事务中扣减数据库库存并创建订单。
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private TransactionTemplate transactionTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    /** 队列名：必须和 seckill.lua 里 XADD 的 key 完全一致 */
    private static final String STREAM_KEY = "seckill:stream";
    /** 消费者组名 */
    private static final String GROUP_NAME = "g1";
    /** 消费者名 */
    private static final String CONSUMER_NAME = "c1";

    @PostConstruct
    private void init() {
        initStreamGroup();
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行 Lua 脚本：判断库存、一人一单，有资格则扣 Redis 库存并发消息到 Stream
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 2.不为 0，代表没有购买资格
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 3.返回订单 id，数据库订单由后台线程异步创建
        return Result.ok(orderId);
    }

    /**
     * 启动时确保消费组存在，否则 XREADGROUP 会抛 NOGROUP 异常
     * 用 XGROUP CREATE key group 0 MKSTREAM：即使 stream 还没被创建也不会报错
     */
    private void initStreamGroup() {
        try {
            stringRedisTemplate.execute((RedisCallback<Object>) connection -> connection.execute(
                    "XGROUP", "CREATE".getBytes(StandardCharsets.UTF_8),
                    STREAM_KEY.getBytes(StandardCharsets.UTF_8),
                    GROUP_NAME.getBytes(StandardCharsets.UTF_8),
                    "0".getBytes(StandardCharsets.UTF_8),
                    "MKSTREAM".getBytes(StandardCharsets.UTF_8)));
            log.info("已创建消费者组 {} -> {}", GROUP_NAME, STREAM_KEY);
        } catch (Exception e) {
            // BUSYGROUP：组已存在，属正常情况
            log.info("消费者组 {}-{} 已存在，跳过创建", STREAM_KEY, GROUP_NAME);
        }
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // 1.获取消息队列中的订单信息
                    // XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS seckill:stream >
                    List<MapRecord<String, Object, Object>> list = readOrders(ReadOffset.lastConsumed(), true);
                    // 2.没有消息，继续下一次循环
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    // 3.处理订单并 ACK
                    handleRecord(list.get(0));
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // Redis 不可用时（连接被关闭 / 重连中），这里不能立刻重试，先退避
                    sleepQuietly(1000L);
                    // 4.补偿处理 pending-list 里没有被 ACK 的消息
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // 读取 pending-list 中的消息：XREADGROUP GROUP g1 c1 COUNT 1 STREAMS seckill:stream 0
                    List<MapRecord<String, Object, Object>> list = readOrders(ReadOffset.from("0"), false);
                    // 没有积压消息，结束补偿，回到主循环
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    handleRecord(list.get(0));
                } catch (Exception e) {
                    log.error("处理 pending-list 异常", e);
                    // 关键：必须退避并退出，否则 Redis 挂掉时会变成无 sleep 的死循环刷屏
                    sleepQuietly(1000L);
                    break;
                }
            }
        }

        /** 统一的读取入口，避免流名 / 组名 / 消费者名写错 */
        private List<MapRecord<String, Object, Object>> readOrders(ReadOffset offset, boolean blocking) {
            StreamReadOptions options = StreamReadOptions.empty().count(1);
            if (blocking) {
                options = options.block(Duration.ofSeconds(2));
            }
            return stringRedisTemplate.opsForStream().read(
                    Consumer.from(GROUP_NAME, CONSUMER_NAME),
                    options,
                    StreamOffset.create(STREAM_KEY, offset)
            );
        }

        /** 解析消息 -> 创建订单 -> XACK */
        private void handleRecord(MapRecord<String, Object, Object> record) {
            Map<Object, Object> value = record.getValue();
            VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
            // 创建订单（失败会抛异常，此时不 ACK，消息留在 pending-list 里等下次补偿）
            createVoucherOrder(voucherOrder);
            // 确认消息 XACK seckill:stream g1 <id>
            stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP_NAME, record.getId());
        }

        private void sleepQuietly(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 在数据库中扣库存并创建订单
     * 1. 加锁在事务外：先提交事务再释放锁，避免其他线程在提交前读到"还没有订单"
     * 2. 用 TransactionTemplate 而不是 @Transactional：这里是类内部调用，
     *    不经过 Spring 代理，@Transactional 不会生效
     */
    private void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        if (!lock.tryLock()) {
            log.error("不允许重复下单！userId={}", userId);
            return;
        }
        try {
            transactionTemplate.executeWithoutResult(status -> {
                // 1.一人一单
                int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
                if (count > 0) {
                    log.error("不允许重复下单！userId={}, voucherId={}", userId, voucherId);
                    return;
                }
                // 2.扣减库存：update ... set stock = stock - 1 where voucher_id = ? and stock > 0
                boolean success = seckillVoucherService.update()
                        .setSql("stock = stock - 1")
                        .eq("voucher_id", voucherId).gt("stock", 0)
                        .update();
                if (!success) {
                    log.error("库存不足！voucherId={}", voucherId);
                    return;
                }
                // 3.创建订单：失败时抛出异常，扣减的库存随事务一起回滚
                save(voucherOrder);
            });
        } finally {
            lock.unlock();
        }
    }
}
