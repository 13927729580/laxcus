/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

/**
 * 列属性的基本成员，标记数据库表中列属性的唯一性。<br>
 * 
 */
public class Spike implements Serializable, Cloneable, Comparable<Spike> {

	private static final long serialVersionUID = 2574232542267184343L;

	/** 列标识，从1开始，在建表时自动顺序分配 **/
	private short columnId;

	/** 列数据类型 (see Type class) **/
	private byte type;

	/** 列名，忽略大小写。字符限制为英文字母，数字和下划线 **/
	private String name;

	/**
	 * 
	 */
	public Spike() {
		super();
		this.columnId = 0;
		this.type = 0;
	}

	/**
	 * @param spike
	 */
	public Spike(Spike spike) {
		this();
		this.set(spike);
	}

	/**
	 * 复制参数
	 * @param spike
	 */
	public void set(Spike spike) {
		this.columnId = spike.columnId;
		this.type = spike.type;
		this.name = new String(spike.name);
	}

	/**
	 * 设置列编号(在数据库表定义中的排列位置)
	 * 
	 * @param id
	 */
	public void setColumnId(short id) {
		this.columnId = id;
	}

	/**
	 * 返回列编号
	 * 
	 * @return short
	 */
	public short getColumnId() {
		return this.columnId;
	}

	/**
	 * 设置列名称
	 * 
	 * @param s
	 */
	public void setName(String s) {
		this.name = s;
	}

	/**
	 * 设置列名称
	 * 
	 * @param b
	 * @param off
	 * @param len
	 */
	public void setName(byte[] b, int off, int len) {
		if (len < 1) {
			name = null;
		} else {
			name = new String(b, off, len);
		}
	}

	/**
	 * 返回列名称
	 * 
	 * @return
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * 设置列数据类型
	 * 
	 * @param b
	 */
	public void setType(byte b) {
		this.type = b;
	}

	/**
	 * 返回列数据类型
	 * 
	 * @return
	 */
	public byte getType() {
		return this.type;
	}

	/*
	 * 克隆对象
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Spike(this);
	}
	
	/*
	 * 根据列标识号排列位置
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Spike atom) {
		return (columnId < atom.columnId ? -1 : (atom.columnId > columnId ? 1 : 0));
	}

}