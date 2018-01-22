/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct;

/**
 * 
 * 数据平均分布命名接口(此接口只用于CONDUCT计算)
 * 
 * 接口位于CALL节点上，数据从每个DATA节点上产生后，向CALL节点返回一个数据分布图，
 * CALL节点汇总全部数据图谱，实现数据平均分配，通知WORK节点到DATA节点上取数据
 * 
 */
public class BalanceObject extends NamingObject {

	private static final long serialVersionUID = 2306664229633598341L;

	/**
	 * default
	 */
	public BalanceObject() {
		super();
	}
	
	/**
	 * 初始化名称
	 * @param name
	 */
	public BalanceObject(String name) {
		this();
		this.setNaming(name);
	}

	/**
	 * @param object
	 */
	public BalanceObject(BalanceObject object) {
		super(object);
	}
	
	/*
	 * 复制BalanceObject对象
	 * 
	 * @see com.lexst.sql.statement.dc.NamingObject#duplicate()
	 */
	@Override
	public Object duplicate() {
		return new BalanceObject(this);
	}
}