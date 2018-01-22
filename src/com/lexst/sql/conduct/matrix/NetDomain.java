/**
 * 
 */
package com.lexst.sql.conduct.matrix;

import java.io.*;
import java.util.*;

import com.lexst.util.*;
import com.lexst.util.host.SiteHost;

/**
 * 汇集多个节点上的数据图谱，形成主机地址->数据图谱的映射关系。<br>
 * 
 */
public class NetDomain implements Serializable ,Cloneable{
	
	private static final long serialVersionUID = -7434325634091237214L;
	
	/** 主机地址 -> 数据图谱 */
	private Map<SiteHost, DiskArea> areas = new TreeMap<SiteHost, DiskArea>();

	/**
	 * default
	 */
	public NetDomain() {
		super();
	}

	/**
	 * @param domain
	 */
	public NetDomain(NetDomain domain) {
		this();
		this.add(domain);
	}

	/**
	 * 保存一块磁盘数据区域
	 * @param host
	 * @param jobid
	 * @param field
	 * @return
	 */
	public boolean add(long jobid, SiteHost host, int timeout, DiskField field) {
		DiskArea area = areas.get(host);
		if (area == null) {
			area = new DiskArea(jobid, host, timeout);
			areas.put((SiteHost) host.clone(), area);
		}
		return area.add(field);
	}

	/**
	 * 保存一组数据
	 * @param domain
	 */
	protected int add(NetDomain domain) {
		int size = 0;
		for (DiskArea area : domain.areas.values()) {
			long jobid = area.getJobid();
			SiteHost host = area.getHost();
			int timeout = area.getTimeout();
			for (DiskField field : area.list()) {
				boolean success = this.add(jobid, host, timeout, field);
				if (success) size++;
			}
		}
		return size;
	}

	/**
	 * 返回主机地址
	 * @return
	 */
	public Set<SiteHost> keySet() {
		return areas.keySet();
	}

	/**
	 * 返回磁盘文件区域集
	 * @param host
	 * @return
	 */
	public DiskArea get(SiteHost host) {
		return areas.get(host);
	}

	/**
	 * 统计数据块的字节总长度
	 * @return
	 */
	public long length() {
		long count = 0;
		for (DiskArea file : areas.values()) {
			count += file.length();
		}
		return count;
	}

	/**
	 * 节点是否空
	 * @return
	 */
	public boolean isEmpty() {
		return areas.isEmpty();
	}

	/**
	 * 节点地址成员数
	 * @return
	 */
	public int size() {
		return areas.size();
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new NetDomain(this);
	}

	/**
	 * @return
	 */
	public byte[] build() {
		// 输出分布区域
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		for (DiskArea area : areas.values()) {
			byte[] b = area.build();
			buff.write(b, 0, b.length);
		}
		byte[] data = buff.toByteArray();

		// 重置数据
		buff.reset();
		// 设置数据流长度
		byte[] b = Numeric.toBytes(data.length);
		buff.write(b, 0, b.length);
		// 写入数据并且返回字节流
		buff.write(data, 0, data.length);
		return buff.toByteArray();
	}

	/**
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		// 检查数据流长度
		if (seek + 4 > end) {
			throw new SizeOutOfBoundsException("size missing!");
		}
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;

		// 解析分布图
		int tail = seek + size;
		if (tail > end) {
			throw new SizeOutOfBoundsException("size missing!");
		}
		while (seek < tail) {
			DiskArea area = new DiskArea();
			int ret = area.resolve(b, seek, tail - seek);
			seek += ret;
			// 保存数据
			areas.put((SiteHost) area.getHost().clone(), area);
		}

		return seek - off;
	}
}