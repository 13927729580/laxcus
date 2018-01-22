/**
 *
 */
package com.lexst.sql.chunk;

import java.io.*;
import java.util.*;

import com.lexst.sql.*;
import com.lexst.sql.index.range.*;

/**
 * 数据块属性信息，包括：<br>
 * <1> 数据块标识<br>
 * <2> 数据块级块(主块或者从块)<br>
 * <3> 数据块状态(已经封闭或者未封闭) <br>
 * <4> KEY值范围集合
 *
 */
public final class ChunkAttribute implements Serializable, Comparable<ChunkAttribute> {

	private static final long serialVersionUID = 1L;

	/** 数据块编号 */
	private long chunkid;

	/** 数据块级别(主块或者从块) */
	private byte rank;

	/** 数据块状态(未封闭或者封闭) */
	private byte status;

	/** 列标识号 -> 索引范围 */
	private Map<Short, IndexRange> mapIndex = new TreeMap<Short, IndexRange>();

	/*
	 *
	 */
	protected ChunkAttribute() {
		super();
		chunkid = 0L; // 初始定义为无效状态:0
		rank = 0;
		status = 0;
	}

	/**
	 * @param chunkid
	 */
	public ChunkAttribute(long chunkid) {
		this();
		this.setId(chunkid);
	}

	/**
	 * @param chunkid
	 * @param rank
	 * @param status
	 */
	public ChunkAttribute(long chunkid, byte rank, byte status) {
		this(chunkid);
		this.setRank(rank);
		this.setStatus(status);
	}

	/**
	 * set chunk identity
	 * @param id
	 */
	public void setId(long id) {
		this.chunkid = id;
	}
	
	/**
	 * get chunk identity
	 * 
	 * @return long
	 */
	public long getId() {
		return this.chunkid;
	}

	/**
	 * chunk rank
	 * @param value
	 */
	public void setRank(byte value) {
		if (value == Type.PRIME_CHUNK || value == Type.SLAVE_CHUNK) {
			rank = value;
		} else {
			throw new IllegalArgumentException("invalid rank!");
		}
	}

	/**
	 * get chunk rank
	 * @return byte
	 */
	public byte getRank() {
		return this.rank;
	}

	/**
	 * set chunk status (complete or other)
	 * @param value
	 */
	public void setStatus(byte value) {
		if (value == Type.COMPLETE_CHUNK || value == Type.INCOMPLETE_CHUNK) {
			this.status = value;
		} else {
			throw new IllegalArgumentException("invalid status!");
		}
	}

	/**
	 * get chunk status
	 * @return byte
	 */
	public byte getStatus() {
		return this.status;
	}
	
	/**
	 * complete status
	 * @return boolean
	 */
	public boolean isComplete() {
		return status == Type.COMPLETE_CHUNK;
	}

	/**
	 * uncomplete status
	 * @return boolean
	 */
	public boolean isIncomplete() {
		return status == Type.INCOMPLETE_CHUNK;
	}

	/**
	 * save a index range
	 * @param range
	 * @return boolean
	 */
	public boolean add(IndexRange range) {
		short columnId = range.getColumnId();
		if (mapIndex.containsKey(columnId)) {
			return false;
		}
		return mapIndex.put(columnId, range) == null;
	}

	/**
	 * index list
	 * @return java.util.Collection<IndexRange>
	 */
	public Collection<IndexRange> list() {
		return mapIndex.values();
	}

	/**
	 * column identity set
	 * @return java.util.Set<short>
	 */
	public Set<Short> keys() {
		return mapIndex.keySet();
	}

	/**
	 * find a index
	 * @param columnId
	 * @return
	 */
	public IndexRange find(short columnId) {
		return mapIndex.get(columnId);
	}

	/**
	 * clear all
	 */
	public void clear() {
		mapIndex.clear();
	}

	/**
	 * empty status
	 * @return boolean
	 */
	public boolean isEmpty() {
		return mapIndex.isEmpty();
	}

	/**
	 * element count
	 * @return int
	 */
	public int size() {
		return mapIndex.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != ChunkAttribute.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((ChunkAttribute) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (chunkid >>> 32 ^ chunkid);
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(ChunkAttribute attribute) {
		return (chunkid < attribute.chunkid ? -1 : (chunkid == attribute.chunkid ? 0 : 1));
	}

}