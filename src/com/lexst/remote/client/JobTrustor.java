/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.remote.client;

import java.nio.*;
import java.util.*;

import com.lexst.log.client.*;
import com.lexst.site.call.*;
import com.lexst.util.lock.*;

/**
 * 连接任务的托管服务器，监督、管理、维护连接任务
 */
public abstract class JobTrustor {

	/** 单向锁(一个时间只允许一个线程操作) */
	protected SingleLock lock = new SingleLock();
	
	/** xxxClient线程在一次工作完成后，是否继续保持在线程状态。默认是FALSE。 */
	private boolean keepThread;

	/** 给xxxClient分配编号，每增加一个xxxClient递增一次  **/
	protected int serial;

	/** 有效的xxxClient句柄统计。在启动时记录，随着xxxClient结束递减。到0表示全部工作完成 **/
	protected int jobs;

	/** 数据头信息 */
	protected ReturnTag tag = new ReturnTag();
	
	/** xxxClient写入的数据，按编号独立保存，输出时合并 **/
	protected Map<Integer, ByteBuffer> mapBuffer = new TreeMap<Integer, ByteBuffer>();

	/**
	 * default
	 */
	public JobTrustor() {
		super();
		this.serial = 0;
		this.jobs = 0;
		this.setKeepThread(false);
	}
	
	/**
	 * xxxClient是否持续保持线程状态
	 * 
	 * @param b
	 */
	public void setKeepThread(boolean b) {
		this.keepThread = b;
	}

	/**
	 * 判断Client是否继续保持线程状态
	 * 
	 * @return
	 */
	public boolean isKeepThread() {
		return this.keepThread;
	}
	
	/**
	 * 预定义线程数量，初始化时设置。对并发过程(调用launch方法时)无效
	 * @param i
	 */
	public void setJobs(int i) {
		this.jobs = i;
	}
	
	/**
	 * 返回当前的剩余线程数
	 * @return
	 */
	public int getJobs() {
		return this.jobs;
	}

	/**
	 * 返回刻度信息
	 * 
	 * @return
	 */
	public ReturnTag getTag() {
		return this.tag;
	}

	/**
	 * 返回有效的记录数
	 * @return
	 */
	public long getItems() {
		return this.tag.getItems();
	}

	/**
	 * 执行一次延时
	 * @param timeout
	 */
	public synchronized void delay(long timeout) {
		try {
			this.wait(timeout);
		} catch (InterruptedException exp) {
			Logger.error(exp);
		}
	}

	/**
	 * 唤醒线程
	 */
	public synchronized void wakeup() {
		try {
			this.notify();
		} catch (IllegalMonitorStateException exp) {
			Logger.error(exp);
		}
	}
	
	/**
	 * 等待任务完成，这个方法在CALL节点调用
	 */
	public void waiting() {
		while(jobs > 0) {
			this.delay(20);
		}
	}

	/**
	 * 合并结果并且返回(包括信息头)
	 * 
	 * @return - 字节流
	 */
	public byte[] data() {
		// 统计字节流总数(包括头标记和各缓存数据)
		long size = 0L;
		Iterator<Map.Entry<Integer, ByteBuffer>> iterators = mapBuffer.entrySet().iterator();
		while (iterators.hasNext()) {
			int fieldlen = iterators.next().getValue().capacity();
			tag.addFieldSize(fieldlen);
			size += fieldlen;
		}
		
		// 生成头信息
		byte[] b = tag.build();
		size += b.length;

		byte[] data = new byte[(int) size];
		// 写入头信息
		int seek = 0;
		System.arraycopy(b, 0, data, seek, b.length);
		seek += b.length;
				
		// 按照次序，依次写入缓存
		iterators = mapBuffer.entrySet().iterator();
		while (iterators.hasNext()) {
			ByteBuffer buff = iterators.next().getValue();
			if (buff.capacity() == 0) continue;
			b = buff.array();
			System.arraycopy(b, 0, data, seek, b.length);
			seek += b.length;
		}
		return data;
	}

	/**
	 * 返回指定编号对应的数据流
	 * 
	 * @param number - 连接器编号
	 * @return
	 */
	public byte[] data(int number) {
		ByteBuffer buff = mapBuffer.get(number);
		if (buff == null || buff.capacity() == 0) {
			return null;
		}
		return buff.array();
	}

	/**
	 * 写入一个空数组
	 * 
	 * @param number - 连接器编号
	 */
	public void flushEmpty(ThreadClient client) {
		flushTo(client, 0, null, 0, 0);
	}

	/**
	 * DataClient/WorkClient任务完成后，数据回写，收入缓存
	 * 
	 * @param client - 客户端句柄
	 * @param elements - 返回的数据成员数(SQL WHERE检索的记录数或者其它)
	 * @param b - 字节数据
	 * @param off - 字节数组开始位置
	 * @param len - 字节数组长度
	 */
	public void flushTo(ThreadClient client, long elements, byte[] b, int off, int len) {
		int number = client.getNumber();
		lock.lock();
		try {
			// 连接任务减1
			this.jobs--;
			// 累计新增加的成员数
			this.tag.addItem(elements);
			// 数据写入缓存
			ByteBuffer buff = ByteBuffer.allocate(len);
			if (len > 0) {
				buff.put(b, off, len);
			}
			mapBuffer.put(number, buff);
		} catch (Throwable exp) {
			Logger.error(exp);
		} finally {
			lock.unlock();
		}
		
		// 如果不保持线程，通知线线程退出
		if(!this.isKeepThread()) {
			client.interrupt();
		}

		// 连接器工作完成，唤醒等待线程
		if (jobs < 1) {
			this.tag.setEndTime(System.currentTimeMillis());
			this.wakeup();
		}
	}

	/**
	 * 启动存储队列中的全部任务(针对并发处理过程)
	 */
	public abstract void launch();

	/**
	 * 停止全部存储队列中的连接任务(对并发处理过程有效)
	 * 
	 * @param force - TRUE，返回强制关闭SOCKET连接；FALSE，通知线程结束
	 */
	public abstract void disconnect(boolean force);

}