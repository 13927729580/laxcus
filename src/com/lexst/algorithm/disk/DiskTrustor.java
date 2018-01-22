/**
 * 
 */
package com.lexst.algorithm.disk;

import com.lexst.util.host.*;

/**
 * 磁盘数据的委托代理器。<br>
 * 此接口的实现类将完成任务号的分配，数据的写入，读出工作。<br>
 */
public interface DiskTrustor {

	/**
	 * 返回当前节点服务器的地址
	 * 
	 * @return
	 */
	SiteHost getLocal();

	/**
	 * 申请一个任务编号
	 * 
	 * @return
	 */
	long nextJobid();

	/**
	 * 数据在磁盘文件上的保存时间，单位：秒。(超时后数据将被删除)
	 * 
	 * @return
	 */
	int timeout();

	/**
	 * 写数据到磁盘，返回这块数据在文件的下标范围(begin and end)
	 * 
	 * @param jobid
	 * @param mod
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	long[] write(long jobid, int mod, byte[] b, int off, int len);

	/**
	 * 从磁盘中读出指定范围的数据流
	 * 
	 * @param jobid
	 * @param begin
	 * @param end
	 * @return
	 */
	byte[] read(long jobid, int mod, long begin, long end);
}