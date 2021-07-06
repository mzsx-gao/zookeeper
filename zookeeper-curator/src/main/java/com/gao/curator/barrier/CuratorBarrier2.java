package com.gao.curator.barrier;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

public class CuratorBarrier2 {

	static final int SESSION_OUTTIME = 5000;//ms
	
	static DistributedBarrier barrier = null;
	
	public static void main(String[] args) throws Exception {
        final TestingServer server = new TestingServer();
		for(int i = 0; i < 5; i++){
			new Thread(()->{
                try {
                    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
                    CuratorFramework cf = CuratorFrameworkFactory.builder()
                                .connectString(server.getConnectString())
                                .sessionTimeoutMs(SESSION_OUTTIME)
                                .retryPolicy(retryPolicy)
                                .build();
                    cf.start();
                    barrier = new DistributedBarrier(cf, "/super");
                    System.out.println(Thread.currentThread().getName() + "设置barrier!");
                    barrier.setBarrier();	    //设置
                    barrier.waitOnBarrier();	//等待
                    System.out.println("---------开始执行程序----------");
                } catch (Exception e) {
                    e.printStackTrace();
                }
			},"t" + i).start();
		}
		Thread.sleep(2000);
		barrier.removeBarrier();	//释放
	}
}
