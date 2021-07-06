package com.gao.curator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * 控制多个线程同时访问
 */
public class InterProcessSemaphoreDemo {
    private static final int MAX_LEASE = 10;
    private static final String PATH = "/examples/locks";

    public static void main(String[] args) throws Exception {
        FakeLimitedResource resource = new FakeLimitedResource();
        try (TestingServer server = new TestingServer()) {

            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
                    new ExponentialBackoffRetry(1000, 3));
            client.start();

            InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, PATH, MAX_LEASE);
            Collection<Lease> leases = semaphore.acquire(5);
            System.out.println("获取租约数量:" + leases.size());
            Lease lease = semaphore.acquire();
            System.out.println("获取单个租约");

            resource.use();

            Collection<Lease> leases2 = semaphore.acquire(5, 10, TimeUnit.SECONDS);
            System.out.println("获取租约，如果为空则超时： " + leases2);

            System.out.println("释放租约");
            semaphore.returnLease(lease);
            System.out.println("释放集合中的所有租约");
            semaphore.returnAll(leases);
        }
    }
}