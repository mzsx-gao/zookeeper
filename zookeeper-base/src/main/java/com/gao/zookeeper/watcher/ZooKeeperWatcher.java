package com.gao.zookeeper.watcher;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *   名称: ZooKeeperWatcher.java <br>
 *   描述: zookeeper监控功能<br>
 *   类型: JAVA <br>
 *   最近修改时间:2017/9/6 14:32.<br>
 *   @version [版本号, V1.0]
 *   @since 2017/9/6 14:32.
 *   @author gaoshudian
 */
public class ZooKeeperWatcher implements Watcher{

    //定义session失效时间
    private static final int SESSION_TIMEOUT = 100000;

    //zookeeper服务器地址,多个地址","隔开
    private static final String CONNECTION_ADDR = "localhost:2181";

    //定义原子变量
    AtomicInteger seq = new AtomicInteger();

    //信号量设置，用于等待zookeeper连接建立之后 通知阻塞程序继续向下执行
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zk = null;

    /*
      主要测试watch功能:
        ① exists操作上的watch，在被监视的Znode创建、删除或数据更新时被触发。
        ② getData操作上的watch，在被监视的Znode删除或数据更新时被触发。在被创建时不能被触发，因为只有Znode一定存在，getData操作才会成功。
        ③ getChildren操作上的watch，在被监视的Znode的子节点更新或删除，或是这个Znode自身被删除时被触发。
        可以通过查看watch事件类型来区分是Znode，还是他的子节点被删除：NodeDelete表示Znode被删除，NodeChildrenChanged表示子节点被删除
     */
    public static void main(String[] args) throws Exception {

        ZooKeeperWatcher zkWatch = new ZooKeeperWatcher();
        zkWatch.createConnection(CONNECTION_ADDR, SESSION_TIMEOUT);

        //创建父节点
        zkWatch.createPath("/parentNode", System.currentTimeMillis() + "");

        //读取父节点数据
        zkWatch.readData("/parentNode", true);

        // 更新父节点数据
        zkWatch.updateData("/parentNode", System.currentTimeMillis() + "");

        // 读取子节点(getChildren负责设置孩子watch)
        zkWatch.getChildren("/parentNode", true);

        // 创建子节点
        zkWatch.createPath("/parentNode/childNode", System.currentTimeMillis() + "");

        //修改子节点数据
        zkWatch.updateData("/parentNode/childNode", System.currentTimeMillis() + "");

        // 清理节点
        zkWatch.deleteAllTestPath();
        zkWatch.releaseConnection();
    }


    /**
     * 收到来自Server的Watcher通知后的处理。
     */
    @Override
    public void process(WatchedEvent event) {
        System.out.println("进入 process 。。。。。event = " + event);
        if (event == null) {
            return;
        }
        Event.KeeperState keeperState = event.getState();// 连接状态
        Event.EventType eventType = event.getType();// 事件类型
        String path = event.getPath();// 受影响的path
        String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";
        System.out.println(logPrefix + "收到Watcher通知");
        System.out.println(logPrefix + "连接状态:\t" + keeperState.toString());
        System.out.println(logPrefix + "事件类型:\t" + eventType.toString());
        System.out.println(logPrefix + "受影响的path:\t" + path);

        if (Event.KeeperState.SyncConnected == keeperState) {
            if (Event.EventType.None == eventType) {// 成功连接上ZK服务器
                System.out.println(logPrefix + "成功连接上ZK服务器");
                connectedSemaphore.countDown();
            }else if (Event.EventType.NodeCreated == eventType) {//创建节点
                System.out.println(logPrefix + "节点创建");
                this.exists(path, true);
            }else if (EventType.NodeDataChanged == eventType) {//更新节点
                System.out.println(logPrefix + "节点数据更新");
                System.out.println(logPrefix + "数据内容: " + this.readData("/parentNode", true));
            }else if (EventType.NodeChildrenChanged == eventType) {//更新子节点
                System.out.println(logPrefix + "子节点变更");
                System.out.println(logPrefix + "子节点列表：" + this.getChildren("/parentNode", true));
            }else if (EventType.NodeDeleted == eventType) {//删除节点
                System.out.println(logPrefix + "节点 " + path + " 被删除");
            }
        }else if (KeeperState.Disconnected == keeperState) {
            System.out.println(logPrefix + "与ZK服务器断开连接");
        }else if (KeeperState.AuthFailed == keeperState) {
            System.out.println(logPrefix + "权限检查失败");
        }else if (KeeperState.Expired == keeperState) {
            System.out.println(logPrefix + "会话失效");
        }
        System.out.println("--------------------------------------------");

    }

    //创建ZK连接
    public void createConnection(String connectAddr, int sessionTimeout) throws Exception{
        System.out.println("【Main】createConnection开始。。。");
        this.releaseConnection();//关闭zk连接
        zk = new ZooKeeper(connectAddr, sessionTimeout, this);
        System.out.println("【Main】" + "开始连接ZK服务器");
        connectedSemaphore.await();
        System.out.println("【Main】createConnection结束。。。");
    }

    //创建节点
    private void createPath(String path, String data) throws Exception{
        //设置监控(由于zookeeper的监控都是一次性的所以 每次必须设置监控)
        this.zk.exists(path, true);
        String actualPath = this.zk.create(path,data.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT );
        System.out.println("【Main】" + "节点创建成功, Path: " + actualPath +", content: " + data);
    }

    //读取指定节点数据内容
    private String readData(String path, boolean needWatch) {
        try {
            return new String(this.zk.getData(path, needWatch, null));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    //更新指定节点数据内容
    private void updateData(String path, String data) throws Exception{
        Stat stat = this.zk.setData(path, data.getBytes(), -1);
        System.out.println("【Main】" + "更新数据成功，path：" + path + ", stat: " + stat);
    }

    //删除指定节点
    private void deleteNode(String path) throws Exception{
        this.zk.delete(path, -1);
        System.out.println("【Main】" + "删除节点成功，path：" + path);
    }

    //判断指定节点是否存在
    private Stat exists(String path, boolean needWatch) {
        try {
            return this.zk.exists(path, needWatch);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    //获取子节点
    private List<String> getChildren(String path, boolean needWatch) {
        try {
            return this.zk.getChildren(path, needWatch);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    //删除所有节点
    private void deleteAllTestPath() throws Exception{
        if(this.exists("/parentNode/childNode", false) != null){
            this.deleteNode("/parentNode/childNode");
        }
        if(this.exists("/parentNode", false) != null){
            this.deleteNode("/parentNode");
        }
    }

    //关闭ZK连接
    public void releaseConnection() throws Exception{
        if (this.zk != null) {
            this.zk.close();
        }
    }
}