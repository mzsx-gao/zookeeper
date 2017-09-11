package com.gao.zkClient.watcher;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

/**
 * 订阅节点的数据内容变化(只监控节点本身(删除和修改),不监控子节点的任何变化)
 */
public class ZkClientWatcher2 {

	/** zookeeper地址 */
	static final String CONNECT_ADDR = "172.16.35.204:2181,172.16.35.204:2182,172.16.35.204:2183";
	/** session超时时间 */
	static final int SESSION_OUTTIME = 5000;//ms 
	
	
	public static void main(String[] args) throws Exception {
		ZkClient zkc = new ZkClient(new ZkConnection(CONNECT_ADDR), SESSION_OUTTIME);
		
		zkc.createPersistent("/super", "1234");
		
		//订阅节点的数据内容变化(只监控节点本身,不监控子节点的任何变化)
		zkc.subscribeDataChanges("/super", new IZkDataListener() {
			@Override
			public void handleDataDeleted(String path) throws Exception {
				System.out.println("删除的节点为:" + path);
			}
			
			@Override
			public void handleDataChange(String path, Object data) throws Exception {
				System.out.println("变更的节点为:" + path + ", 变更内容为:" + data);
			}
		});
		
		Thread.sleep(3000);
		zkc.writeData("/super", "456", -1);
		Thread.sleep(1000);

		zkc.delete("/super");
		Thread.sleep(Integer.MAX_VALUE);
		
		
	}
}
