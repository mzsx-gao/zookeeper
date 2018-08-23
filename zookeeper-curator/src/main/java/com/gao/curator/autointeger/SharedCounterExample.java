package com.gao.curator.autointeger;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

import com.google.common.collect.Lists;

public class SharedCounterExample implements SharedCountListener{

    private static final int QTY = 5;
    private static final String PATH = "/examples/counter";

    public static void main(String[] args) throws Exception {
        final Random rand = new Random();
        SharedCounterExample example = new SharedCounterExample();
        TestingServer server = new TestingServer();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        SharedCount baseCount = new SharedCount(client, PATH, 0);
        baseCount.addListener(example);
        baseCount.start();

        List<SharedCount> examples = Lists.newArrayList();
        ExecutorService service = Executors.newFixedThreadPool(QTY);
        //模拟5台服务器同时修改该数字
        for (int i = 0; i < QTY; i++) {
            final SharedCount count = new SharedCount(client, PATH, 0);
            examples.add(count);
            service.submit(() ->{
                count.start();
                Thread.sleep(rand.nextInt(1000));
                boolean result = count.trySetCount(count.getVersionedValue(), count.getCount() + 1);
                System.out.println(Thread.currentThread().getName()+".....Increment是否成功:" +result);
                return null;
            });
        }

        service.shutdown();
        service.awaitTermination(5, TimeUnit.MINUTES);

        System.out.println("最终值为:"+ baseCount.getCount());
        for (int i = 0; i < QTY; ++i) {
            examples.get(i).close();
        }
        baseCount.close();

    }

    @Override
    public void stateChanged(CuratorFramework arg0, ConnectionState connectionState) {
        System.out.println("状态改变: " + connectionState.toString());
    }

    @Override
    public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
        System.out.println("Counter's 值改变为: " + newCount);
    }
}