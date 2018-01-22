/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.conduct;

import java.util.*;

/**
 * 
 *
 */
public class FromOutputObject extends OutputObject {

	private static final long serialVersionUID = 8819238044718175681L;

	/** 分布配置任务集合 **/
	private ArrayList<FromPhase> array = new ArrayList<FromPhase>();

	/**
	 * 默认初始化
	 */
	public FromOutputObject() {
		super();
	}

	/**
	 * 初始化并且设置任务命名
	 * 
	 * @param naming
	 */
	public FromOutputObject(String naming) {
		this();
		this.setNaming(naming);
	}

	/**
	 * 复制对象
	 * @param object
	 */
	public FromOutputObject(FromOutputObject object) {
		super(object);
		this.array.addAll(object.array);
	}

	/**
	 * 保存一个"diffuse"阶段FromPhase对象
	 * 
	 * @param phase
	 * @return
	 */
	public boolean addPhase(FromPhase phase) {
		if (naming != null) {
			phase.setNaming(naming);
		}
		return array.add(phase);
	}

	/**
	 * 返回指定下标位置的FromPhase对象
	 * 
	 * @param index
	 * @return
	 */
	public FromPhase getPhase(int index) {
		if (index < 0 || index >= array.size()) {
			return null;
		}
		return array.get(index);
	}

	/**
	 * FromPhase成员数
	 * 
	 * @return
	 */
	public int phases() {
		return array.size();
	}

	/**
	 * 内存收缩到实际尺寸
	 */
	public void trimPhases() {
		this.array.trimToSize();
	}

	public List<FromPhase> list() {
		return this.array;
	}

	public boolean isEmpty() {
		return array.isEmpty();
	}

	public int size() {
		return this.array.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.distribute.OutputObject#clone()
	 */
	@Override
	public Object clone() {
		return new FromOutputObject(this);
	}
}
