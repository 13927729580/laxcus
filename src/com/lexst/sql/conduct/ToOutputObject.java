/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.conduct;

import java.util.*;

/**
 * 保存ToTask对象的集合
 * 
 * 对应规则对"aggregate"阶段的定义，ToOutputObject是一个链表的关系，从首链开始，依次连接
 * 
 */
public class ToOutputObject extends OutputObject {

	private static final long serialVersionUID = 28306649038452699L;

	/** 分布任务集合 */
	private ArrayList<ToPhase> array = new ArrayList<ToPhase>(5);

	/** 下一级"aggregate"操作对象 */
	private ToOutputObject slave;

	/**
	 * default
	 */
	public ToOutputObject() {
		super();
	}
	
	/**
	 * @param naming
	 */
	public ToOutputObject(String naming) {
		this();
		super.setNaming(naming);
	}

	/**
	 * @param object
	 */
	public ToOutputObject(ToOutputObject object) {
		super(object);
		this.array.addAll(object.array);
		if (object.slave != null) {
			this.slave = new ToOutputObject(object.slave);
		}
	}

	/**
	 * 保存一个"aggregate"分布任务
	 * 
	 * @param phase
	 * @return
	 */
	public boolean addPhase(ToPhase phase) {
		if (naming != null) {
			phase.setNaming(naming);
		}
		return array.add(phase);
	}
	
	/**
	 * 返回指定下标位置的ToPhase对象
	 * 
	 * @param index
	 * @return
	 */
	public ToPhase getPhase(int index) {
		if (index < 0 || index >= array.size()) {
			return null;
		}
		return array.get(index);
	}

	/**
	 * 返回分布"aggregate"计算任务集合
	 * 
	 * @return
	 */
	public List<ToPhase> list() {
		return this.array;
	}

	/**
	 * 判断空
	 * 
	 * @return
	 */
	public boolean isEmpty() {
		return array.isEmpty();
	}

	/**
	 * 集合成员数
	 * 
	 * @return
	 */
	public int size() {
		return this.array.size();
	}
	
	public int phases() {
		return array.size();
	}

	/**
	 * 收缩到有效空间
	 */
	public void trimPhases() {
		this.array.trimToSize();
	}

	/**
	 * 设置"to - subto"链表成员的排列序号(从TO根对象开始设置，下标是0)
	 * 
	 * @param index
	 */
	private void setLinkIndex(int index) {
		for (ToPhase task : array) {
			task.setLinkIndex(index);
		}
		if (slave != null) {
			slave.setLinkIndex(index + 1);
		}
	}
	
	public void doLinkIndex() {
		this.setLinkIndex(0);
	}

	/**
	 * 设置子级对象
	 * 
	 * @param object
	 */
	public void setLast(ToOutputObject object) {
		if (this.slave != null) {
			this.slave.setLast(object);
		} else {
			this.slave = object;
		}
	}

	/**
	 * 是否有子级对象
	 * 
	 * @return
	 */
	public boolean hasNext() {
		return this.slave != null;
	}

	/**
	 * 返回子级ToOutputObject对象
	 * 
	 * @return
	 */
	public ToOutputObject next() {
		return this.slave;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new ToOutputObject(this);
	}
}