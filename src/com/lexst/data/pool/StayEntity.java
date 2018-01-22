/**
 * 
 */
package com.lexst.data.pool;

import java.util.*;

/**
 * SQL INSERT操作的异步数据存储区域记录集合。<br>
 * 记录第一段数据在磁盘文件上的位置
 * 
 */
final class StayEntity {

	/** 磁盘文件号 **/
	private int diskid;

	/** 磁盘文件名 **/
	private String filename;

	/** 文件填弃满标记 **/
	private boolean completed;

	/** 子块集合 **/
	private ArrayList<StayNode> array = new ArrayList<StayNode>(50);

	/**
	 * default
	 */
	public StayEntity(int diskid, String filename) {
		super();
		this.setDiskid(diskid);
		this.setFilename(filename);
		this.completed = false;
	}

	/**
	 * 设置磁盘文件号
	 * 
	 * @param i
	 */
	public void setDiskid(int i) {
		this.diskid = i;
	}

	/**
	 * 返回磁盘文件号
	 * 
	 * @return
	 */
	public int getDiskid() {
		return this.diskid;
	}

	/**
	 * 设置磁盘文件名
	 * 
	 * @param s
	 */
	public void setFilename(String s) {
		this.filename = s;
	}

	/**
	 * 返回磁盘文件名
	 * 
	 * @return
	 */
	public String getFilename() {
		return this.filename;
	}

	/**
	 * 文件存储块完成标记
	 * 
	 * @param b
	 */
	public void setCompleted(boolean b) {
		this.completed = b;
	}

	/**
	 * 判断文件存储块是否完成
	 * 
	 * @return
	 */
	public boolean isCompleted() {
		return this.completed;
	}

	/**
	 * 追加一段异步数据存储点
	 * 
	 * @param node
	 * @return
	 */
	public boolean add(StayNode node) {
		return array.add(node);
	}

	/**
	 * 弹出存储块
	 * 
	 * @return
	 */
	public StayNode poll() {
		if (array.size() > 0) {
			return array.remove(0);
		}
		return null;
	}

	public int size() {
		return array.size();
	}

	public boolean isEmpty() {
		return array.isEmpty();
	}

}