/**
 *
 */
package com.lexst.remote.client.data;

import java.util.*;

import com.lexst.remote.client.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.statement.*;

/**
 * 多个Data节点连接任务的托管服务器
 * 
 * 提供一个或者多个DataClient的连接、监督、管理任务
 *
 */
public final class DataTrustor extends JobTrustor {
	
	/** DataClient 连接句柄集合 **/
	private ArrayList<DataTask> array = new ArrayList<DataTask>(10);

	/**
	 * default
	 */
	public DataTrustor() {
		super();
	}

	/**
	 * 指定向量空间尺寸
	 * 
	 * @param capacity - DataTask尺寸
	 */
	public DataTrustor(int capacity) {
		super();
		this.array.ensureCapacity(capacity);
	}

	/**
	 * 并发模式，保存SQL计算句柄(收到完成配置后由launch方法启动)
	 * @param client
	 * @param method
	 */
	public void push(DataClient client, SQLMethod method) {
		client.setNumber(this.serial++);
		array.add(new DataTask(client, method));
	}

	/**
	 * 并发模式，保存分布计算句柄(收到完成配置后由launch方法启动)
	 * @param client
	 * @param phase
	 */
	public void push(DataClient client, FromPhase phase) {
		// 给每一个客户端设置序列号
		client.setNumber(this.serial++);
		// 保存句柄等待启动
		array.add(new DataTask(client, phase));
	}
	
	/**
	 * 启动SQL操作
	 * @param client
	 * @param method
	 */
	public void send(DataClient client, SQLMethod method) {
		// 设置任务开始时间
		if(serial == 0) {
			tag.setBeginTime(System.currentTimeMillis());
		}
		// 分配序列号
		client.setNumber(this.serial++);
		
		switch (method.getMethod()) {
		case Compute.SELECT_METHOD:
			client.select(this, (Select) method);
			break;
		case Compute.DELETE_METHOD:
			client.delete(this, (Delete) method);
			break;
		default:
			// error
		}
		
		// 唤醒线程
		client.wakeupThread();
	}
	
	/**
	 * 启动conduct操作
	 * 
	 * @param client
	 * @param phase
	 */
	public void send(DataClient client, FromPhase phase) {
		// 设置任务开始时间
		if (serial == 0) {
			tag.setBeginTime(System.currentTimeMillis());
		}
		// 给每一个客户端分配序列号
		client.setNumber(this.serial++);
		// 保存句柄等待启动
		client.conduct(this, phase);
		// 启动或者唤醒线程
		client.wakeupThread();
	}
	
	/**
	 * 连接器成员数
	 * 
	 * @return
	 */
	public int size() {
		return array.size();
	}

	/*
	 * 启动队列中的任务
	 * 
	 * @see com.lexst.remote.client.JobTrustor#launch()
	 */
	public void launch() {
		// 有效的连接任务数
		jobs = array.size();
		// 数据开始时间
		tag.setBeginTime(System.currentTimeMillis());
		// 依次启动全部任务
		for (DataTask task : array) {
			switch (task.method.getMethod()) {
			case Compute.SELECT_METHOD:
				task.client.select(this, (Select) task.method);
				break;
			case Compute.DELETE_METHOD:
				task.client.delete(this, (Delete) task.method);
				break;
			case Compute.CONDUCT_METHOD:
				task.client.conduct(this, task.phase);
				break;
			}
			// clear resource
			task.release();
		 }
	}

	/*
	 * 停止全部连接
	 * @see com.lexst.remote.client.JobTrustor#disconnect(boolean)
	 */
	public void disconnect(boolean force) {
		for (DataTask task : array) {
			if (force) {
				if (task.client.isRunning()) {
					task.client.stop(); // exit thread
				} else {
					task.client.close();
				}
			} else {
				task.client.unlock();
			}
			// clear resource
			task.release();
		}
	}
	
}