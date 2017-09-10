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
    private static final String CONNECT_ADDR = "192.168.0.206:2181,192.168.0.207:2181,192.168.0.208:2181";
    //session超时时间
    private static final int SESSION_OUTTIME = 2000;//ms

    /**
     *  创建父节点
     *  节点创建类型(CreateMode)
     *     1、PERSISTENT:持久化节点
     *     2、PERSISTENT_SEQUENTIAL:顺序自动编号持久化节点，这种节点会根据当前已存在的节点数自动加 1
     *     3、EPHEMERAL:临时节点客户端,session超时这类节点就会被自动删除
     *     4、EPHEMERAL_SEQUENTIAL:临时自动编号节点
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
     *  设置某个znode上的数据时如果为-1，跳过版本检查
     */
    @Test
    public void updateNode() throws Exception{
        ZooKeeper zk =getZk();
        //修改节点的值
        zk.setData("/testRoot", "modify data root2".getBytes(), -1);
        byte[] data = zk.getData("/testRoot", false, null);
        System.out.println(new String(data));

        zk.close();
    }

    /**
     *  删除节点
     *  说明
     *     1、版本号不一致,无法进行数据删除操作.
     *     2、如果版本号与znode的版本号不一致,将无法删除,是一种乐观加锁机制;如果将版本号设置为-1,不会去检测版本,直接删除.
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

    /**
     * 获取zookeeper连接
     * 1.关于connectString服务器地址配置
     *     格式: 192.168.1.1:2181,192.168.1.2:2181,192.168.1.3:2181
     *     这个地址配置有多个ip:port之间逗号分隔,底层操作
     *     ConnectStringParser connectStringParser =  new ConnectStringParser(“192.168.1.1:2181,192.168.1.2:2181,192.168.1.3:2181”);
     *     这个类主要就是解析传入地址列表字符串，将其它保存在一个ArrayList中
     *     ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();
     *     接下去，这个地址列表会被进一步封装成StaticHostProvider对象，并且在运行过程中，一直是这个对象来维护整个地址列表。
     *     ZK客户端将所有Server保存在一个List中，然后随机打乱(这个随机过程是一次性的)，并且形成一个环，具体使用的时候，从0号位开始一个一个使用。
     *     因此，Server地址能够重复配置，这样能够弥补客户端无法设置Server权重的缺陷，但是也会加大风险。
     *
     *  2.客户端和服务端会话说明
     *     ZooKeeper中，客户端和服务端建立连接后，会话随之建立，生成一个全局唯一的会话ID(Session ID)。
     *     服务器和客户端之间维持的是一个长连接，在SESSION_TIMEOUT时间内，服务器会确定客户端是否正常连接(客户端会定时向服务器发送heart_beat，服务器重置下次SESSION_TIMEOUT时间)。
     *     因此，在正常情况下，Session一直有效，并且ZK集群所有机器上都保存这个Session信息。
     *     在出现网络或其它问题情况下（例如客户端所连接的那台ZK机器挂了，或是其它原因的网络闪断）,客户端与当前连接的那台服务器之间连接断了,
     *     这个时候客户端会主动在地址列表（实例化ZK对象的时候传入构造方法的那个参数connectString）中选择新的地址进行连接。
     *
     *  3.会话时间
     *     客户端并不是可以随意设置这个会话超时时间，在ZK服务器端对会话超时时间是有限制的，主要是minSessionTimeout和maxSessionTimeout这两个参数设置的。
     *     如果客户端设置的超时时间不在这个范围，那么会被强制设置为最大或最小时间。 默认的Session超时时间是在2 * tickTime ~ 20 * tickTime
     */
    public ZooKeeper getZk() throws Exception{

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