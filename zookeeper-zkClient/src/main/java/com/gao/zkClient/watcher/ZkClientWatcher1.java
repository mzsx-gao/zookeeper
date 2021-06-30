package com.gao.zkClient.watcher;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

/**
 * 订阅节点的信息变化(只监控创建节点、删除节点、添加子节点、删除子节点)
 */
public class ZkClientWatcher1 {

	/** zookeeper地址 */
//	static final String CONNECT_ADDR = "172.16.35.204:2181,172.16.35.204:2182,172.16.35.204:2183";
    static final String CONNECT_ADDR = "47.103.97.241:2181";
	/** session超时时间 */
	static final int SESSION_OUTTIME = 500000;//ms
	
	
	public static void main(String[] args) throws Exception {
		ZkClient zkc = new ZkClient(new ZkConnection(CONNECT_ADDR), SESSION_OUTTIME);
		
		//订阅节点的信息变化(只监控创建节点、删除节点、添加子节点、删除子节点)
		zkc.subscribeChildChanges("/super", new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
				System.out.println("parentPath: " + parentPath);
				System.out.println("currentChilds: " + currentChilds);
			}
		});
		
		Thread.sleep(1000);
		
		zkc.createPersistent("/super");
		Thread.sleep(1000);

        zkc.writeData("/super", "456", -1);
        Thread.sleep(1000);

		zkc.createPersistent("/super" + "/" + "c1", "c1内容");
		Thread.sleep(1000);
		
		zkc.createPersistent("/super" + "/" + "c2", "c2内容");
		Thread.sleep(1000);		
		
		zkc.delete("/super/c2");
		Thread.sleep(1000);	
		
		zkc.deleteRecursive("/super");
		Thread.sleep(Integer.MAX_VALUE);
		
		
	}
}
