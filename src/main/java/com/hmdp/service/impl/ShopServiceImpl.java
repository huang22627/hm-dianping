package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryById(Long id) {
        Shop shop = queryWithLogicalExpire(id);
        if (shop==null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
   public Shop queryWithLogicalExpire(Long id){

        //查询缓存
       String key="cache:shop" + id;
       String shopJson = stringRedisTemplate.opsForValue().get(key);
       //未命中直接返回
       if(StrUtil.isBlank(shopJson)) {
           return null;
       }
       //命中
       //查看是否过期
       RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
       Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
       LocalDateTime expireTime = redisData.getExpireTime();
       //未过期
       if (expireTime.isAfter(LocalDateTime.now())) {
             return shop;
       }
       //过期
       //进行缓存重建
       //获取锁
       String lockKey="lock:shop:" + id;
       if (tryLock(lockKey)) {

           //成功，再次检测缓存是否过期，未过期返回
           if (expireTime.isAfter(LocalDateTime.now())) {
               return shop;
           }
           // 过期开启一个线程执行查询并更新缓存
           CACHE_REBUILD_EXECUTOR.submit(()->{
               try {
                   saveShop2Redis(id,20L);
               } catch (Exception e) {
                   throw new RuntimeException(e);
               } finally {
                   unlock(lockKey);
               }
           });
       }

       //失败，返回过期数据

        return shop;

   }


public void saveShop2Redis(Long id,Long expireSeconds){
    Shop shop= getById(id);
    String key="cache:shop" + id;
    RedisData redisData = new RedisData();
    redisData.setData(shop);
    redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
    stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));


}




    public Shop queryWithMutex(Long id){
        String key="cache:shop" + id;
        //查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //存在转成shop对象直接返回
           return JSONUtil.toBean(shopJson, Shop.class);

        }
        if (shopJson!=null) {
            return null;
        }
        //实现缓存重构
        //获取互斥锁
        String lockKey="lock:shop:" + id;
        Shop shop=null;
        try {
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //不存在根据id去数据库查
            shop = getById(id);
            Thread.sleep(200);
            //商铺不存在写入空值并返回
            if (shop==null) {
                stringRedisTemplate.opsForValue().set(key,"",3L, TimeUnit.MINUTES);
                return null;
            }
            //存在写入redis并返回
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
        finally {
           unlock(lockKey);
        }
        return shop;

    }



    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id==null) {
            return Result.fail("店铺id不能为空");
        }
        updateById(shop);
        stringRedisTemplate.delete("cache:shop" + id);
        return Result.ok();
    }


    public Result queryByIdBack(Long id) {
        String key="cache:shop" + id;
        //查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //存在转成shop对象直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }
        if (shopJson!=null) {
            return Result.fail("店铺信息不存在");
        }
        //不存在根据id去数据库查
        Shop shop = getById(id);
        //商铺不存在写入空值并返回
        if (shop==null) {
            stringRedisTemplate.opsForValue().set(key,"",3L, TimeUnit.MINUTES);
            return Result.fail("商户不存在");
        }
        //存在写入redis并返回
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);
        return Result.ok(shop);
    }
}
