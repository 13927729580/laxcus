/**
 * @email admin@wigres.com
 *
 */
package com.lexst.site.call;

import java.io.*;
import java.util.*;

import com.lexst.util.*;

/**
 * CALL节点返回的数据结果最前面的标记信息
 *
 */
public final class ReturnTag {

	/** 行记录总数或者其它单元总数信息 */
	private long items;
	
	/** 信息的开始或者结束时间 **/
	private long beginTime, endTime;

	/** 数据域的段落数 */
	private int fields;
	/** 每个段的数据长度 */
	private List<Integer> array = new ArrayList<Integer>();	
	
	/**
	 * default
	 */
	public ReturnTag() {
		super();
		this.reset();
	}
	
	/**
	 * 重置参数
	 */
	public void reset() {
		this.items = 0L;
		this.beginTime = endTime = 0L;
		this.fields = 0;
		this.array.clear();
	}

	/**
	 * 增加单元数
	 * @param i
	 */
	public void addItem(long i) {
		this.items += i;
	}
	
	/**
	 * 返回单元总数
	 * @return
	 */
	public long getItems() {
		return this.items;
	}
	
	/**
	 * 区域成员数
	 * @return
	 */
	public int getFields(){
		return this.fields;
	}
	
	/**
	 * 增加一段区域字节长度
	 * @param len
	 * @return
	 */
	public boolean addFieldSize(int len) {
		return this.array.add(len);
	}
	
	/**
	 * 返回区域字节集合
	 * @return
	 */
	public List<Integer> getFieldSizes() {
		return this.array;
	}
	
	/**
	 * 返回数据区域字节总长度
	 * @return
	 */
	public long getSize() {
		long size = 0L;
		for(int len : array) {
			size += len;
		}
		return size;
	}
	
	/**
	 * 设置计算开始时间
	 * 
	 * @param i
	 */
	public void setBeginTime(long i) {
		this.beginTime = i;
	}
	
	/**
	 * 返回计算开始时间
	 * @return
	 */
	public long getBeginTime() {
		return this.beginTime;
	}
	
	/**
	 * 设置计算结束时间
	 * @param i
	 */
	public void setEndTime(long i) {
		this.endTime = i;
	}
	
	/**
	 * 返回计算结束时间
	 * @return
	 */
	public long getEndTime() {
		return this.endTime;
	}
	
	/**
	 * 本次计算耗时(单位：毫秒)
	 * 
	 * @return
	 */
	public long usedTime() {
		return endTime - beginTime;
	}
	
	/**
	 * 生成数据流
	 * 
	 * @return
	 */
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(256);

		byte[] b = Numeric.toBytes(this.items);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(this.beginTime);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(this.endTime);
		buff.write(b, 0, b.length);

		// 区域数
		b = Numeric.toBytes(this.fields = array.size());
		buff.write(b, 0, b.length);
		// 每段区域的长度
		for (Integer size : array) {
			b = Numeric.toBytes(size.intValue());
			buff.write(b, 0, b.length);
		}

		return buff.toByteArray();
	}
	
	/**
	 * 解析数据流
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		if (seek + 28 > end) {
			throw new SizeOutOfBoundsException("tag sizeout!");
		}

		this.items = Numeric.toLong(b, seek, 8);
		seek += 8;
		this.beginTime = Numeric.toLong(b, seek, 8);
		seek += 8;
		this.endTime = Numeric.toLong(b, seek, 8);
		seek += 8;

		// 区域统计
		fields = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if(seek + fields * 4 > end) {
			throw new SizeOutOfBoundsException("tag sizeout!");
		}
		// 每段区域字节长度
		for(int i = 0; i <fields; i++) {
			int size = Numeric.toInteger(b, seek, 4);
			seek += 4;
			array.add(size);
		}
		
		return seek - off;
	}
	
}