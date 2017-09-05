package com.gao.zookeeper.base;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;


/**
 *   名称: ZookeeperBase.java <br>
 *   描述: JAVA操作zookeeper<br>
 *   类型: JAVA <br>
 *   最近修改时间:2017/9/5 10:31.<br>
 *   @version [版本号, V1.0]
 *   @since 2017/9/5 10:31.
 *   @author gaoshudian
 */
public class ZookeeperBase {


    //zookeeper地址
    static final String CONNECT_ADDR = "172.16.35.204:2181,172.16.35.204:2182,172.16.35.204:2183";
    //session超时时间
    static final int SESSION_OUTTIME = 2000;//ms

    /**
     *  创建父节点
     */
    @Test
    public void createParentNode() throws Exception{
        System.out.println("nihao");
        ZooKeeper zk =getZk();
        //创建父节点
		String parentNode = zk.create("/testRoot", "testRoot".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建父节点parentNode:"+parentNode);

        zk.close();
    }

    /**
     *  创建子节点
     */
    @Test
    public void createChildNode() throws Exception{
        ZooKeeper zk =getZk();
        //创建子节点
        String childNode =zk.create("/testRoot/children", "children data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建子节点parentNode:"+childNode);

        zk.close();
    }

    /**
     *  获取节点信息
     */
    @Test
    public void getNode()throws Exception{
        ZooKeeper zk =getZk();
        //获取节点信息
        byte[] data = zk.getData("/testRoot", false, null);
        System.out.println(new String(data));
        System.out.println(zk.getChildren("/testRoot", false));

        zk.close();
    }

    /**
     *  更新节点
     */
    @Test
    public void updateNode() throws Exception{
        ZooKeeper zk =getZk();
        //修改节点的值
        zk.setData("/testRoot", "modify data root".getBytes(), -1);
        byte[] data = zk.getData("/testRoot", false, null);
        System.out.println(new String(data));

        zk.close();
    }

    /**
     *  删除节点
     */
    @Test
    public void DeleteNode() throws Exception{
        ZooKeeper zk =getZk();
        zk.delete("/testRoot/children", -1);
		System.out.println(zk.exists("/testRoot/children", false));

        zk.close();
    }

    /**
     *  判断节点是否存在
     */
    @Test
    public void NodeIsExists() throws Exception{
        ZooKeeper zk =getZk();
        System.out.println(zk.exists("/testRoot/children", false));

        zk.close();
    }

    public static  ZooKeeper getZk() throws Exception{

        //信号量，阻塞程序执行，用于等待zookeeper连接成功，发送成功信号
        final CountDownLatch connectedSemaphore = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //获取事件的状态
                KeeperState keeperState = event.getState();
                EventType eventType = event.getType();
                //如果是建立连接
                if (KeeperState.SyncConnected == keeperState) {
                    if (EventType.None == eventType) {
                        //如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
                        connectedSemaphore.countDown();
                        System.out.println("zk 建立连接");
                    }
                }
            }
        });
        //进行阻塞
        connectedSemaphore.await();
        return zk;
    }


}