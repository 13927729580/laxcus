/**
 * 
 */
package com.lexst.sql.conduct.matrix;

import java.io.*;
import java.util.*;

import com.lexst.util.*;
import com.lexst.util.host.SiteHost;

/**
 * 磁盘数据图谱，在DATA/WORK节点产生，一次diffuse or aggregate计算只产生一个DiskArea。<br>
 * 一个数据图谱由多段子图谱(DiskField)组成。<br><br>
 * 
 * 每个数据图谱由一个任务号(jobid)表示它的唯一性。
 */
public class DiskArea implements Serializable, Cloneable, Comparable<DiskArea> {

	private static final long serialVersionUID = 5291054498535265130L;

	/** 任务号(job identity)，系统分配，保证唯一 **/
	private long jobid;

	/** DATA/WORK节点数据等待时间，单位：秒 (超过指定时间，DATA将删除存储记录) **/
	private int timeout;

	/** DATA/WORK节点地址  **/
	private SiteHost host;

	/** DATA/WORK节点上的分布数据区域  **/
	private ArrayList<DiskField> array = new ArrayList<DiskField>();

	/**
	 * 初始化磁盘文件数据集
	 */
	public DiskArea() {
		super();
		this.jobid = 0L;
		this.timeout = 0;
	}

	/**
	 * 复制磁盘文件数据集
	 * @param object
	 */
	public DiskArea(DiskArea object) {
		this();
		this.setJobid(object.jobid);
		this.setTimeout(object.timeout);
		this.setHost(object.host);
		this.add(object.array);
	}

//	/**
//	 * 初始化设置任务工作号
//	 * 
//	 * @param jobid
//	 */
//	public DiskArea(long jobid) {
//		this();
//		this.setJobid(jobid);
//	}

	/**
	 * 初始化并且任务工作号、目标地址，目标数据超时时间
	 * 
	 * @param jobid
	 * @param host
	 * @param timeout
	 */
	public DiskArea(long jobid, SiteHost host, int timeout) {
		this();
		this.setJobid(jobid);
		this.setHost(host);
		this.setTimeout(timeout);
	}

	/**
	 * 设置任务工作号
	 * 
	 * @param i
	 */
	public void setJobid(long i) {
		this.jobid = i;
	}

	/**
	 * 返回任务号
	 * 
	 * @return
	 */
	public long getJobid() {
		return this.jobid;
	}

	/**
	 * 设置DATA/WORK主机地址
	 * 
	 * @param s
	 */
	public void setHost(SiteHost s) {
		if (s != null) {
			this.host = new SiteHost(s);
		}
	}

	/**
	 * 返回DATA/WORK主机地址
	 * 
	 * @return
	 */
	public SiteHost getHost() {
		return this.host;
	}

	/**
	 * 设置数据超时时间
	 * 
	 * @param i
	 */
	public void setTimeout(int i) {
		this.timeout = i;
	}

	/**
	 * 返回数据超时时间
	 * 
	 * @return
	 */
	public int getTimeout() {
		return this.timeout;
	}

	/**
	 * 保存一项磁盘数据记录
	 * @param field
	 * @return
	 */
	public boolean add(DiskField field) {
		if (!array.contains(field)) {
			return array.add((DiskField) field.clone());
		}
		return false;
	}

	/**
	 * 保存一组磁盘数据记录
	 * @param fields
	 * @return
	 */
	public int add(Collection<DiskField> fields) {
		int size = array.size();
		for (DiskField field : fields) {
			add(field);
		}
		return array.size() - size;
	}

	/**
	 * 删除一项磁盘数据记录
	 * @param object
	 * @return
	 */
	public boolean remove(DiskField object) {
		return this.array.remove(object);
	}

	/**
	 * 返回磁盘数据记录集合
	 * @return
	 */
	public List<DiskField> list() {
		return this.array;
	}
	
	/**
	 * 收缩内存占用
	 */
	public void trim() {
		this.array.trimToSize();
	}

	/**
	 * 统计磁盘文件的总长度
	 * 
	 * @return
	 */
	public long length() {
		long len = 0;
		for (DiskField field : array) {
			len += field.length();
		}
		return len;
	}

	public boolean isEmpty() {
		return array.isEmpty();
	}

	public int size() {
		return array.size();
	}

	/**
	 * 生成数据流
	 * 
	 * @return
	 */
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(5120);
		// 任务号
		byte[] b = Numeric.toBytes(jobid);
		buff.write(b, 0, b.length);
		// 节点超时
		b = Numeric.toBytes(timeout);
		buff.write(b, 0, b.length);
		// 节点主机地址
//		byte[] ip = host.getIP().getBytes();
//		byte[] ip = host.getAddress().getAddress();
		
//		byte[] ip = host.getAddress().bits();
//		b = Numeric.toBytes(ip.length);
//		buff.write(b, 0, b.length);
//		buff.write(ip, 0, ip.length);
//		b = Numeric.toBytes(host.getTCPort());
//		buff.write(b, 0, b.length);
//		b = Numeric.toBytes(host.getUDPort());
//		buff.write(b, 0, b.length);
		
		// 节点址
		b = host.build();
		buff.write(b, 0, b.length);
		
		// 分布区域成员
		int elements = array.size();
		b = Numeric.toBytes(elements);
		buff.write(b, 0, b.length);
		if (elements > 0) {
			for (DiskField field : array) {
				field.build(buff);
			}
		}

		return buff.toByteArray();
	}

	/**
	 * 解析数据流
	 * 
	 * @param b
	 * @param off
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		if(seek + 16 > end) {
			throw new IndexOutOfBoundsException("dcarea indexout");
		}
		// 任务标记
		jobid = Numeric.toLong(b, seek, 8);
		seek += 8;
		// 超时
		timeout = Numeric.toInteger(b, seek, 4);
		seek += 4;
		
//		// 主机地址
//		int size = Numeric.toInteger(b, seek, 4);
//		seek += 4;
//		if(seek + size > end) {
//			throw new IndexOutOfBoundsException("dcarea indexout");
//		}
		
//		String ip = new String(b, seek, size);
		
//		byte[] ip = Arrays.copyOfRange(b, seek, seek + size);
//		seek += size;
//		if(seek + 12 > end) {
//			throw new IndexOutOfBoundsException("dcarea indexout");
//		}
//		int tcport = Numeric.toInteger(b, seek, 4);
//		seek += 4;
//		int udport = Numeric.toInteger(b, seek, 4);
//		seek += 4;
//		try {
//			host = new SiteHost(ip, tcport, udport);
//		} catch (UnknownHostException e) {
//			
//		}
		
		// 节点地址
		host = new SiteHost();
		int size = host.resolve(b, seek, end - seek);
		seek += size;

		// 分布成员
		int elements = Numeric.toInteger(b, seek, 4);
		seek += 4;
		for (int i = 0; i < elements; i++) {
			DiskField field = new DiskField();
			size = field.resolve(b, seek, end - seek);
			seek += size;
			array.add(field);
		}

		return seek - off;
	}
	
	/*
	 * 比较对象是否一致
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || !(object instanceof DiskArea)) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((DiskArea) object) == 0;
	}

	/*
	 * 返回哈希码
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (jobid >>> 32 & jobid);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new DiskArea(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DiskArea area) {
		int ret = (jobid < area.jobid ? -1 : (jobid > area.jobid ? 1 : 0));
		if (ret == 0) {
			ret = (host == null ? -1 : host.compareTo(area.host));
		}
		return ret;
	}
}