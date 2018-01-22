/**
 *
 */
package com.lexst.sql.chunk;

import java.util.*;
import java.io.Serializable;

/**
 * 数据块集合。保存CHUNK的64位标识
 */
public class ChunkSet implements Serializable, Cloneable {

	private static final long serialVersionUID = -5297188243623030264L;

	/** 数据块标识集合  */
	private Set<Long> array = new TreeSet<Long>();

	/**
	 * 初始化数据块标识号
	 */
	public ChunkSet() {
		super();
	}

	/**
	 * @param chunkid
	 */
	public ChunkSet(long chunkid) {
		this();
		this.add(chunkid);
	}

	/**
	 * @param s
	 */
	public ChunkSet(long[] s) {
		this();
		this.add(s);
	}
	
	/**
	 * 复制集合
	 * @param object
	 */
	public ChunkSet(ChunkSet object) {
		this();
		this.add(object.array);
	}

	/**
	 * 增加一个数据块标识号(0值无效)
	 * @param chunkid
	 * @return
	 */
	public boolean add(long chunkid) {
		return chunkid != 0L && array.add(chunkid);
	}
	
	/**
	 * 删除一个数据块标识号
	 * @param chunkid
	 * @return
	 */
	public boolean remove(long chunkid) {
		return array.remove(chunkid);
	}

	/**
	 * 增加一组数据块标识号
	 * @param set
	 * @return
	 */
	public int add(Set<Long> set) {
		int size = array.size();
		 array.addAll(set);
		return array.size() - size;
	}

	/**
	 * 增加一组数据块标识号
	 * @param s
	 * @return
	 */
	public int add(long[] s) {
		int size = array.size();
		for (int i = 0; s != null && i < s.length; i++) {
			this.add(s[i]);
		}
		return array.size() - size;
	}

	/**
	 * 增加一组数据块标识号
	 * @param set
	 * @return
	 */
	public int add(ChunkSet set) {
		return this.add(set.array);
	}

	/**
	 * 删除一组数据块标识号
	 * @param s
	 * @return
	 */
	public int remove(long[] s) {
		int size = array.size();
		for (int i = 0; s != null && i < s.length; i++) {
			this.remove(s[i]);
		}
		return size - array.size();
	}

	/**
	 * 删除一组数据块标识号
	 * @param set
	 * @return
	 */
	public int remove(Set<Long> set) {
		int size = array.size();
		array.removeAll(set);
		return size - array.size();
	}
	
	/**
	 * 逻辑"与"操作：保留相同，取消不同
	 * 
	 * @param set
	 */
	public void AND(Set<Long> set) {
		array.retainAll(set);
	}

	/**
	 * 逻辑"与"操作
	 * @param set
	 */
	public void AND(ChunkSet set) {
		this.AND(set.array);
	}

	/**
	 * 逻辑"或"操作：重叠的保留一个，不重叠的也保留
	 * 
	 * @param set
	 */
	public void OR(Set<Long> set) {
		array.addAll(set);
	}

	/**
	 * 逻辑"或"操作
	 * @param set
	 */
	public void OR(ChunkSet set) {
		this.OR(set.array);
	}

	/**
	 * 返回标识号集合
	 * @return
	 */
	public Set<Long> list() {
		return this.array;
	}

	/**
	 * 判断是否存在
	 * @param chunkid
	 * @return
	 */
	public boolean exists(long chunkid) {
		return array.contains(chunkid);
	}

//	public boolean contains(long chunkid) {
//		return array.contains(chunkid);
//	}

	/**
	 * 清空集合
	 */
	public void clear() {
		array.clear();
	}

	/**
	 * 集合尺寸
	 * @return
	 */
	public int size() {
		return array.size();
	}

	/**
	 * 空集合判断
	 * @return
	 */
	public boolean isEmpty() {
		return array.isEmpty();
	}

	/**
	 * 返回升序排序的标识号数组
	 * @return
	 */
	public long[] toArray() {
		ArrayList<Long> a = new ArrayList<Long>(array);
		long[] s = new long[a.size()];
		for (int i = 0; i < s.length; i++) {
			s[i] = a.get(i);
		}
		return s;
	}

	/*
	 * 克隆副本
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new ChunkSet(this);
	}
}