package com.hmdp.service;

public interface ILock {
    /**
     *  获取锁
     * @param seconds 秒数
     * @return
     */
    boolean tryLock(long seconds);

    /**
     * 释放锁
     */
    void unlock();
}
