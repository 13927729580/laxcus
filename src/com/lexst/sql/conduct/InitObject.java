/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct;

/**
 * 任务启动前，初始化相关接口和数据。此接口位于CALL节点上
 * 
 */
public class InitObject extends NamingObject {

	private static final long serialVersionUID = -4816667419362055433L;

	/**
	 * default
	 */
	public InitObject() {
		super();
	}

	/**
	 * 初始化对象并且设置名称
	 * 
	 * @param naming
	 */
	public InitObject(String naming) {
		this();
		this.setNaming(naming);
	}

	/**
	 * 复制对象名称
	 * 
	 * @param object
	 */
	public InitObject(InitObject object) {
		super(object);
	}

	/*
	 * 克隆对象
	 * 
	 * @see com.lexst.sql.distribute.NamingObject#duplicate()
	 */
	@Override
	public Object duplicate() {
		return new InitObject(this);
	}
}