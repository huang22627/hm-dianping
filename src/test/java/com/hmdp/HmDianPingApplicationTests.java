package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private RedisIdWorker redisIdWorker;
    private ExecutorService es= Executors.newFixedThreadPool(500);
    @Test
    void testid() throws InterruptedException {
        CountDownLatch latch=new CountDownLatch(300);
        Runnable task=()->{
            for (int i = 0; i < 100; i++) {
                long order = redisIdWorker.nextId("order");
                System.out.println("id="+order);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.err.println("time="+(end-begin));
    }
    @Test
    void test(){
        LocalDateTime time = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
        long l = time.toEpochSecond(ZoneOffset.UTC);
        log.info("时间{}",String.valueOf(l));

    }
    @Test
    void testSave(){

        shopService.saveShop2Redis(1L,20L);
    }


}
