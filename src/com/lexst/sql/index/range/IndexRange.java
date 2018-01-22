/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * index range
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 6/23/2009
 * 
 * @see com.lexst.sql.index.range
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.index.range;

import java.io.Serializable;

import com.lexst.sql.*;


public abstract class IndexRange implements Serializable, Cloneable, Comparable<IndexRange> {

	private static final long serialVersionUID = -1427970117152139488L;

	/** 索引类型 */
	protected byte type;

	/** 列标识号 */
	protected short columnId;

	/** 数据块标识号 */
	protected long chunkId;

	/**
	 *
	 */
	protected IndexRange() {
		super();
	}

	/**
	 * @param range
	 */
	public IndexRange(IndexRange range) {
		this();
		this.type = range.type;
		this.columnId = range.columnId;
		this.chunkId = range.chunkId;
	}

	/**
	 * @param type
	 */
	public IndexRange(byte type) {
		this();
		this.setType(type);
	}

	/**
	 *
	 * @param chunkId
	 * @param columnId
	 */
	public IndexRange(byte type, long chunkId, short columnId) {
		this();
		this.setType(type);
		this.setChunkId(chunkId);
		this.setColumnId(columnId);
	}

	public void setColumnId(short id) {
		this.columnId = id;
	}
	public short getColumnId() {
		return this.columnId;
	}

	public void setChunkId(long id) {
		this.chunkId = id;
	}
	public long getChunkId() {
		return this.chunkId;
	}

	public boolean isType(byte type) {
		return Type.SHORT_INDEX <= type && type <= Type.DOUBLE_INDEX;
	}

	public void setType(byte b) {
		if (!isType(b)) {
			throw new IllegalArgumentException("invalid index type!");
		}
		this.type = b;
	}

	public byte getType() {
		return type;
	}

	public boolean isShort() {
		return type == Type.SHORT_INDEX;
	}

	public boolean isInteger() {
		return type == Type.INTEGER_INDEX;
	}

	public boolean isLong() {
		return type == Type.LONG_INDEX;
	}

	public boolean isFloat() {
		return type == Type.FLOAT_INDEX;
	}

	public boolean isDouble() {
		return type == Type.DOUBLE_INDEX;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || !(object instanceof IndexRange)) {
			return false;
		} else if (object == this) {
			return true;
		}

		return compareTo((IndexRange) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (chunkId ^ columnId);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(IndexRange index) {
		if (index == null) {
			return 1;
		}
		return (chunkId < index.chunkId ? -1 : (chunkId > index.chunkId ? 1 : 0));
	}

}