/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * chunk entity class
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 2/2/2009
 * 
 * @see com.lexst.sql.chunk
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.chunk;

import java.io.*;

/**
 * 数据块实例信息，包括：<br>
 * <1> 数据块标识，一个64位的整数，由TOP节点分配，全局唯一<br>
 * <2> 数据块长度(通常是64M，最大不超过2G)<br>
 * <3> 数据块最后修改时间<br>
 * <4> 数据块的MD5码(通过MD5码检查两个相同标识号的数据块是否一致)<br>
 *
 */
public class ChunkStatus implements Serializable, Comparable<ChunkStatus> { 

	private static final long serialVersionUID = -7964429659909147714L;

	/** 数据块标识 */
	private long identity;

	/** 数据块长度 */
	private long length;

	/** 数据块最后修改日期 */
	private long lastModified;
	
	/** 数据块MD5码 */
	private long md5low;
	private long md5high;

	/**
	 * 
	 */
	public ChunkStatus() {
		super();
	}
	
	/**
	 * @param id
	 * @param length
	 * @param modified
	 */
	public ChunkStatus(long id, long length, long modified) {
		this();
		this.setId(id);
		this.setLength(length);
		this.setLastModified(modified);
	}
	
	/**
	 * 设置数据块标识
	 * 
	 * @param id
	 */
	public void setId(long id) {
		this.identity = id;
	}

	public long getId() {
		return this.identity;
	}
	
	public void setLength(long len) {
		this.length = len;
	}
	public long getLength() {
		return this.length;
	}
	
	public void setLastModified(long time) {
		this.lastModified = time;
	}

	public long getLastModified() {
		return this.lastModified;
	}
	
	public void setMD5(long low, long high) {
		this.md5low = low;
		this.md5high = high;
	}

	public long getMD5High() {
		return this.md5high;
	}

	public long getMD5Low() {
		return this.md5low;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != ChunkStatus.class) {
			return false;
		} else if (obj == this) {
			return true;
		}
		ChunkStatus chunk = (ChunkStatus) obj;
		return chunk.identity == identity;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return (int) ((identity >>> 32) ^ identity);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(ChunkStatus chunk) {
		return (identity < chunk.identity ? -1 : (identity > chunk.identity ? 1 : 0));
	}
}