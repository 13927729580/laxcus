/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com, All rights reserved
 * 
 * index table
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 1/2/2010
 * 
 * @see com.lexst.sql.chunk
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.chunk;

import java.io.*;
import java.util.*;

import com.lexst.sql.schema.*;

/**
 *
 * DATA主机的数据库表索引数据
 */
public final class IndexTable implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 表名 **/
	private Space space;

	/** 数据块标识 -> 数据块属性 */
	private Map<Long, ChunkAttribute> attributes = new TreeMap<Long, ChunkAttribute>();

	/**
	 *
	 */
	public IndexTable() {
		super();
	}

	/**
	 * @param s
	 */
	public IndexTable(Space s) {
		this();
		this.setSpace(s);
	}

	public void setSpace(Space s) {
		this.space = new Space(s);
	}

	public Space getSpace() {
		return this.space;
	}

	/**
	 * @param sheet
	 * @return
	 */
	public boolean add(ChunkAttribute sheet) {
		long chunkId = sheet.getId();
		if (attributes.containsKey(chunkId)) {
			return false;
		}
		return attributes.put(chunkId, sheet) == null;
	}

	/**
	 * @param chunkId
	 * @return
	 */
	public boolean remove(long chunkId) {
		return attributes.remove(chunkId) != null;
	}

	/**
	 * @param chunkId
	 * @return
	 */
	public boolean contains(long chunkId) {
		return attributes.get(chunkId) != null;
	}

	/**
	 * @return
	 */
	public Set<Long> keys() {
		return attributes.keySet();
	}

	/**
	 * @param chunkId
	 * @return
	 */
	public ChunkAttribute find(long chunkId) {
		return attributes.get(chunkId);
	}

	/**
	 *
	 * @return
	 */
	public Collection<ChunkAttribute> list() {
		return attributes.values();
	}

	/**
	 * clear data
	 */
	public void clear() {
		attributes.clear();
	}

	public int size() {
		return attributes.size();
	}

	public boolean isEmpty() {
		return attributes.isEmpty();
	}
}