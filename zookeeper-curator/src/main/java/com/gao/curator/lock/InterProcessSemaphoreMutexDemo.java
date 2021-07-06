package com.gao.curator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
  不可重入共享锁—Shared Lock
  这个锁和上面的InterProcessMutex相比，就是少了Reentrant的功能，也就意味着它不能在同一个线程中重入。这个类是InterProcessSemaphoreMutex

  运行后发现，有且只有一个client成功获取第一个锁(第一个acquire()方法返回true)，然后它自己阻塞在第二个acquire()方法，获取第二个锁超时；
  其他所有的客户端都阻塞在第一个acquire()方法超时并且抛出异常。这样也就验证了InterProcessSemaphoreMutex实现的锁是不可重入的
 */
public class InterProcessSemaphoreMutexDemo {
    private InterProcessSemaphoreMutex lock;
    private final FakeLimitedResource resource;
    private final String clientName;

    private static final String PATH = "/examples/locks";

    public InterProcessSemaphoreMutexDemo(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
        this.resource = resource;
        this.clientName = clientName;
        this.lock = new InterProcessSemaphoreMutex(client, lockPath);
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " 不能得到互斥锁");
        }
        System.out.println(clientName + " 已获取到互斥锁");
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " 不能得到互斥锁");
        }
        System.out.println(clientName + " 再次获取到互斥锁");
        try {
            System.out.println(clientName + " 得到锁");
            resource.use(); //access resource exclusively
        } finally {
            System.out.println(clientName + " 释放锁");
            lock.release(); // always release the lock in a finally block
//            lock.release(); // 获取锁几次 释放锁也要几次
        }
    }

    public static void main(String[] args) throws Exception {
        final FakeLimitedResource resource = new FakeLimitedResource();
        ExecutorService service = Executors.newFixedThreadPool(5);
        final TestingServer server = new TestingServer();
        try {
            for (int i = 0; i < 5; ++i) {
                final int index = i;
                service.submit(()->{
                    CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
                            new ExponentialBackoffRetry(1000, 3));
                    try {
                        client.start();
                        final InterProcessSemaphoreMutexDemo example = new InterProcessSemaphoreMutexDemo(
                                client, PATH, resource, "Client " + index);
                        for (int j = 0; j < 5; ++j) {
                            example.doWork(10, TimeUnit.SECONDS);
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    } finally {
                        CloseableUtils.closeQuietly(client);
                    }
                    return null;
                });
            }
            service.shutdown();
            service.awaitTermination(10, TimeUnit.MINUTES);
        } finally {
            CloseableUtils.closeQuietly(server);
        }
    }
}