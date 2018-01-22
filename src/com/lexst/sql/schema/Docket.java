/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.schema;

import java.io.*;

/**
 * 锚点：数据库表中的列描述(数据库表名称和列标识号的组合)。<br>
 * 
 */
public final class Docket implements Serializable, Cloneable, Comparable<Docket> {

	private static final long serialVersionUID = 4429499283336268810L;

	/** 数据表名称 */
	private Space space;

	/** 列标识 */
	private short columnId;

	/** 散列码 */
	private int hash;

	/**
	 * default
	 */
	public Docket() {
		super();
		this.columnId = 0;
		this.hash = 0;
	}

	/**
	 * 复制对象
	 * @param docket
	 */
	public Docket(Docket docket) {
		this();
		this.set(docket.space, docket.columnId);
	}
	
	/**
	 * 
	 * @param space
	 * @param columnId
	 */
	public Docket(Space space, short columnId) {
		this();
		this.set(space, columnId);
	}
	
	/**
	 * @param schema
	 * @param table
	 * @param columnId
	 */
	public Docket(String schema, String table, short columnId) {
		this(new Space(schema, table), columnId);
	}

	/**
	 * 设置参数
	 * @param s
	 * @param id
	 */
	public void set(Space s, short id) {
		this.space = new Space(s);
		this.columnId = id;
	}

	/**
	 * 返回表名
	 * @return
	 */
	public Space getSpace() {
		return this.space;
	}
	
	/**
	 * 返回数据库名称
	 * 
	 * @return
	 */
	public String getSchema() {
		return this.space.getSchema();
	}

	/**
	 * 返回表名称
	 * 
	 * @return
	 */
	public String getTable() {
		return this.space.getTable();
	}

	/**
	 * 返回列标识
	 * @return
	 */
	public short getColumnId() {
		return this.columnId;
	}

	/*
	 * 比较是否一致
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != Docket.class) {
			return false;
		} else if (object == this) {
			return true;
		}

		return this.compareTo((Docket) object) == 0;
	}

	/*
	 * 返回散列码
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (this.hash == 0 && space != null) {
			this.hash = space.hashCode() ^ columnId;
		}
		return this.hash;
	}

	/*
	 * 克隆对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Docket(this);
	}

	/* 
	 * 比较排列
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Docket deck) {
		// 未定义状态时
		if (space == null || columnId < 1) {
			return -1;
		}

		int ret = space.compareTo(deck.space);
		if (ret == 0) {
			ret = (columnId < deck.columnId ? -1 : (columnId > deck.columnId ? 1 : 0));
		}
		return ret;
	}
}