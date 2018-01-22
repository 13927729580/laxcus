/**
 *
 */
package com.lexst.thread;

import java.util.*;

import com.lexst.util.host.*;

/**
 * 远程停止节点地址表
 */
final class ShutdownSheet {

	/** 保存被授权远程关闭的网络地址 **/
	private ArrayList<Address> array = new ArrayList<Address>();

	/**
	 * default
	 */
	public ShutdownSheet() {
		super();
	}

	/**
	 * 保存一个网络地址
	 * 
	 * @param address
	 * @return
	 */
	public boolean add(Address address) {
		if (address == null || array.contains(address)) {
			return false;
		}
		return this.array.add((Address) address.clone());
	}

	/**
	 * 保存一组网络地址
	 * 
	 * @param addresses
	 * @return
	 */
	public int add(Address[] addresses) {
		int size = array.size();
		for (int i = 0; addresses != null && i < addresses.length; i++) {
			add(addresses[i]);
		}
		return array.size() - size;
	}

	/**
	 * 返回地址列表
	 * 
	 * @return
	 */
	public List<Address> list() {
		return this.array;
	}

	/**
	 * 检查指定的网络地址是否存在
	 * 
	 * @param address
	 * @return
	 */
	public boolean contains(Address address) {
		return this.array.contains(address);
	}

	/**
	 * 清空地址表
	 */
	public void clear() {
		this.array.clear();
	}

	/**
	 * 返回地址表成员数
	 * 
	 * @return
	 */
	public int size() {
		return this.array.size();
	}

	/**
	 * 地址表是否为空
	 * 
	 * @return
	 */
	public boolean isEmpty() {
		return this.array.isEmpty();
	}

	/**
	 * 收缩地址表内存空间
	 */
	public void trim() {
		this.array.trimToSize();
	}

}