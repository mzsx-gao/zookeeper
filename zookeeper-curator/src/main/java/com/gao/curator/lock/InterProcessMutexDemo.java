package com.gao.curator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 可重入共享锁-Shared Reentrant Lock
 * Shared意味着锁是全局可见的， 客户端都可以请求锁。 Reentrant和JDK的ReentrantLock类似，即可重入,意味着同一个客户端在拥有锁的同时，可以多次获取，不会被阻塞。
 *
 * 下面例子说明：
 * 1.生成5个client,每个client重复执行5次 请求锁–访问资源–释放锁的过程。每个client都在独立的线程中。 结果可以看到，锁是随机的被每个实例排他性的使用。
 * 2.既然是可重用的，你可以在一个线程中多次调用acquire(),在线程拥有锁时它总是返回true。
 * 3.你不应该在多个线程中用同一个InterProcessMutex， 你可以在每个线程中都生成一个新的InterProcessMutex实例，它们的path都一样，这样它们可以共享同一个锁
 */
public class InterProcessMutexDemo {
    private InterProcessMutex lock;
    private final FakeLimitedResource resource;
    private final String clientName;

    private InterProcessMutexDemo(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
        this.resource = resource;
        this.clientName = clientName;
        this.lock = new InterProcessMutex(client, lockPath);
    }

    private void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " could not acquire the lock");
        }
        try {
            System.out.println(clientName + " 获得锁");
            resource.use(); //access resource exclusively
        } finally {
            System.out.println(clientName + " 释放锁");
            lock.release(); // always release the lock in a finally block
        }
    }

    private static final String PATH = "/examples/locks";

    public static void main(String[] args) throws Exception {
        final FakeLimitedResource resource = new FakeLimitedResource();
        ExecutorService threadPools = Executors.newFixedThreadPool(5);
        final TestingServer server = new TestingServer();
        try {
            for (int i = 0; i < 5; ++i) {
                final int index = i;
                Callable<Void> task = new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
                        try {
                            client.start();
                            final InterProcessMutexDemo example = new InterProcessMutexDemo(client, PATH, resource, "Client " + index);
                            for (int j = 0; j < 5; ++j) {
                                example.doWork(10, TimeUnit.SECONDS);
                            }
                        } catch (Throwable e) {
                            e.printStackTrace();
                        } finally {
                            CloseableUtils.closeQuietly(client);
                        }
                        return null;
                    }
                };
                threadPools.submit(task);
            }
            //shutdown和awaitTermination为接口ExecutorService定义的两个方法，一般情况配合使用来关闭线程池。
            //1.shutdown方法：平滑的关闭ExecutorService，当此方法被调用时，ExecutorService停止接收新的任务并且等待已经提交
            // 的任务（包含提交正在执行和提交未执行）执行完成。当所有提交任务执行完毕，线程池即被关闭。
            //2.awaitTermination方法：接收人timeout和TimeUnit两个参数，用于设定超时时间及单位。当等待超过设定时间时，
            // 会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。一般情况下会和shutdown方法组合使用
            threadPools.shutdown();
            threadPools.awaitTermination(10, TimeUnit.MINUTES);
        } finally {
            CloseableUtils.closeQuietly(server);
        }
    }
}
