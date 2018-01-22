/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct;

/**
 * 收集数据，完成最终计算任务。<br>
 * 此对象实例位于最终显示或者存储阶段，即CALL节点或者SQLive、SQLive console及其它终端上<br>
 * 
 */
public class CollectObject extends NamingObject {

	private static final long serialVersionUID = -751935990539195415L;

	/** 数据写入磁盘的文件名 */
	private String writeTo;

	/**
	 * default
	 */
	public CollectObject() {
		super();
	}

	/**
	 * 初始化并设置名称
	 * 
	 * @param name
	 */
	public CollectObject(String name) {
		this();
		this.setNaming(name);
	}

	/**
	 * @param object
	 */
	public CollectObject(CollectObject object) {
		super(object);
		this.setWriteTo(object.writeTo);
	}

	/**
	 * 设置本地文件名
	 * 
	 * @param localfile
	 */
	public void setWriteTo(String localfile) {
		this.writeTo = localfile;
	}

	/**
	 * 返回本地文件名
	 * 
	 * @return
	 */
	public String getWriteTo() {
		return this.writeTo;
	}

	/*
	 * 复制 CollectObject 对象
	 * 
	 * @see com.lexst.sql.statement.dc.NamingObject#duplicate()
	 */
	@Override
	public Object duplicate() {
		return new CollectObject(this);
	}

}