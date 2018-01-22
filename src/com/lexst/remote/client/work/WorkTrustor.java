/**
 * 
 */
package com.lexst.remote.client.work;

import java.util.*;

import com.lexst.remote.client.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;

/**
 * WORK节点连接的托管服务器
 * 
 * 提供任意多个WorkClient连接的启动、监督、停止任务
 *
 */
public final class WorkTrustor extends JobTrustor {
		
	/** WORK任务句柄集合 **/
	private ArrayList<WorkTask> array = new ArrayList<WorkTask>(10);
	
	/**
	 * default
	 */
	public WorkTrustor() {
		super();
	}
	
	/**
	 * 指定任务向量空间尺寸
	 * 
	 * @param capacity - WorkTask尺寸
	 */
	public WorkTrustor(int capacity) {
		super();
		array.ensureCapacity(capacity);
	}

	/**
	 * 向队列中追加一个conduct配置，全部完成时调用launch方法
	 * @param client
	 * @param phase
	 * @param data
	 */
	public void push(WorkClient client, ToPhase phase, NetDomain data) {
		// 给每个client设置一个编号
		client.setNumber(serial++);
		// 保存句柄
		array.add(new WorkTask(client, phase, data));
	}

	/**
	 * 立即启动检索(区别存储完成后的并发处理过程)
	 * @param client
	 * @param phase
	 * @param data
	 */
	public void send(WorkClient client, ToPhase phase, NetDomain domain) {
		// 设置任务开始时间
		if(serial == 0) {
			tag.setBeginTime(System.currentTimeMillis());
		}
		// 给每个client设置一个编号
		client.setNumber(serial++);
		// 连接WORK服务器，执行conduct命令
		client.conduct(this, phase, domain);
		// 如果线程运行，唤醒线程；否则启动线程
		client.wakeupThread();
	}

	/**
	 * 存储队列中的线程数量
	 * 
	 * @return
	 */
	public int size() {
		return array.size();
	}

	/*
	 * 启动队列中的任务
	 * @see com.lexst.remote.client.JobTrustor#launch()
	 */
	@Override
	public void launch() {
		jobs = array.size();
		tag.setBeginTime(System.currentTimeMillis());
		// 连接WORK节点，结果数据写回本地缓存
		for (WorkTask task : array) {
			task.client.conduct(this, task.phase, task.domain);
			task.client.wakeupThread();
		}
	}

	/*
	 * 停止全部连接
	 * @see com.lexst.remote.client.JobTrustor#disconnect(boolean)
	 */
	public void disconnect(boolean force) {
		for (WorkTask task : array) {
			if (force) {
				if (task.client.isRunning()) {
					task.client.stop(); // 关闭线程
				} else {
					task.client.close(); // 关闭SOCKET	
				}
			} else {
				task.client.unlock();	// 解除锁定
			}
		}
	}

}