/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.pool;

import com.lexst.fixp.*;
import com.lexst.fixp.monitor.*;
import com.lexst.util.host.*;

/**
 * 控制节点管理池(管理其它节点的数据池)。位于管理节点上，监督/管理/维护其它节点上的运行工作。
 *
 */
public abstract class ControlPool extends Pool {

//	/** 服务池检查时间间隔 **/
//	private long checkTime;

	/** 超时删除时间(毫秒) **/
	private long deleteTime;

	/** 节点更新超时(毫秒) **/
	private long refreshTimeout;

	/** UDP数据包发送接口 **/
	private IPacketListener listener;
	
	/**
	 * 
	 */
	protected ControlPool() {
		super();
		this.setSiteTimeout(20);
		this.setDeleteTimeout(60);
	}

	/**
	 * 设置UDP数据包监听器
	 * @param s
	 */
	public void setPacketListener(IPacketListener s) {
		this.listener = s;
	}

	/**
	 * 返回UDP数据包监听器
	 * @return
	 */
	public IPacketListener getPacketListener() {
		return this.listener;
	}

	public void setDeleteTimeout(int second) {
		if (second >= 5) {
			deleteTime = second * 1000;
		}
	}

	public long getDeleteTimeout() {
		return this.deleteTime;
	}

	public void setSiteTimeout(int second) {
		if (second >= 5) {
			this.refreshTimeout = second * 1000;
		}
	}

	public int getSiteTimeout() {
		return (int) (this.refreshTimeout / 1000);
	}
	
	
	public long getRefreshTimeout() {
		return this.refreshTimeout;
	}

	/**
	 * 发送超时包到下属节点
	 * 
	 * @param remote
	 * @param local
	 * @return
	 */
	protected boolean sendTimeout(SiteHost remote, SiteHost local, int num) {
		for (int i = 0; i < num; i++) {
			Command command = new Command(Request.NOTIFY, Request.COMEBACK);
			Packet packet = new Packet(command);
			// local listen server address
			packet.addMessage(new Message(Key.SERVER_IP, local.getSpecifyAddress()));
			packet.addMessage(new Message(Key.SERVER_TCPORT, local.getTCPort()));
			packet.addMessage(new Message(Key.SERVER_UDPORT, local.getUDPort()));
			packet.addMessage(new Message(Key.TIMEOUT, refreshTimeout)); // second
			SocketHost address = remote.getPacketHost();
			// send to client
			listener.send(address, packet);
		}
		return true;
	}
}
