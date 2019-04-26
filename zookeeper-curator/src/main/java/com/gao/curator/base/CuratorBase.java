package com.gao.curator.base;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class CuratorBase {
	
	/** zookeeper地址 */
//	static final String CONNECT_ADDR = "192.168.0.206:2181,192.168.0.207:2181,192.168.0.208:2181";
	static final String CONNECT_ADDR = "localhost:2181";
	/** session超时时间 */
	static final int SESSION_OUTTIME = 5000;//ms 
	
	public static void main(String[] args) throws Exception {
	    //建立连接
        CuratorFramework cf = getConnection();
        //创建节点
        createNode(cf);
        //读取节点内容
        readNode(cf);
//        //修改节点内容
        editNode(cf);
//        //查询super节点的子节点
        getChildren(cf);
//        //删除节点
        deleteNode(cf);
//        //绑定回调接口
        bindCallBack(cf);
	}

	//建立连接
	private static CuratorFramework getConnection() throws Exception{
        //1 重试策略：重试时间间隔为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        //2 通过工厂创建连接
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_OUTTIME)
                .connectionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(retryPolicy)
                /*为了实现不同的Zookeeper业务之间的隔离，需要为每个业务分配一个独立的命名空间（NameSpace），即指定一个Zookeeper的根路径（官方术语：为Zookeeper添加“Chroot”特性）。
                  例如（下面的例子）当客户端指定了独立命名空间为“/namespace”，那么该客户端对Zookeeper上的数据节点的操作都是基于该目录进行的。
                  通过设置Chroot可以将客户端应用与Zookeeper服务端的一棵子树相对应，在多个应用共用一个Zookeeper集群的场景下，这对于实现不同应用之间的相互隔离十分有意义*/
//				.namespace("namespace")
                .build();
        cf.start();
        System.out.println("连接状态..."+cf.getState());
        return cf;
    }

    //创建节点(如果父节点不存在,则先创建父节点)
    private static void createNode(CuratorFramework cf) throws Exception{
        System.out.println("在super目录下创建节点c1和c2");
        //建立节点 指定节点类型（不加withMode默认为持久类型节点）、路径、数据内容
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/super/c2","c2内容".getBytes());
    }

    //删除节点
    private static void deleteNode(CuratorFramework cf) throws Exception{
        System.out.println("删除节点super");
        Thread.sleep(2000);
        cf.delete().guaranteed().deletingChildrenIfNeeded().forPath("/super");
    }

    //读取节点
    private static void readNode(CuratorFramework cf) throws Exception{
        String ret1 = new String(cf.getData().forPath("/super/c2"));
		System.out.println("读取节点super/c2的内容:"+ret1);
    }

    //修改节点
    private static void editNode(CuratorFramework cf) throws Exception{
        cf.setData().forPath("/super/c2", "修改c2内容".getBytes());
		String ret2 = new String(cf.getData().forPath("/super/c2"));
		System.out.println("修改节点super/c2后的内容:"+ret2);
    }

    //读取子节点getChildren方法 和 判断节点是否存在checkExists方法
    private static void getChildren(CuratorFramework cf) throws Exception{
        System.out.println("获取节点super的子节点...");
        List<String> list = cf.getChildren().forPath("/super");
        for(String p : list){
            System.out.println("子节点:"+p);
        }
        System.out.println("判断节点super/c3是否存在");
        Stat stat = cf.checkExists().forPath("/super/c3");
        System.out.println(stat);
    }

    //绑定回调函数
    private static void bindCallBack(CuratorFramework cf) throws Exception{
        System.out.println("测试回调接口...");
        ExecutorService pool = Executors.newCachedThreadPool();
        cf.create().creatingParentsIfNeeded().inBackground(
                (curatorFramework,curatorEvent)-> {
                    System.out.println("code:" + curatorEvent.getResultCode());
                    System.out.println("type:" + curatorEvent.getType());
                    System.out.println("线程为:" + Thread.currentThread().getName());
                },
                pool)
                .forPath("/super/c3","c3内容".getBytes());
        Thread.sleep(Integer.MAX_VALUE);
    }

}
