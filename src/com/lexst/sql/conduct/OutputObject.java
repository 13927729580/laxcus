/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.conduct;

import java.io.*;

import com.lexst.util.naming.*;

/**
 * 输出对象集合的基础类
 * 
 */
class OutputObject implements Serializable, Cloneable {

	private static final long serialVersionUID = 1595296167780305882L;
	
	/** 命名对象 **/
	protected Naming naming;

	/**
	 * default
	 */
	protected OutputObject() {
		super();
	}

	/**
	 * @param object
	 */
	protected OutputObject(OutputObject object) {
		this();
		this.setNaming(object.naming);
	}

	/**
	 * 设置任务命名。命名可以忽略大小写，必须在TOP集群唯一。
	 * 
	 * @param s
	 */
	public void setNaming(Naming s) {
		this.naming = new Naming(s);
	}

	/**
	 * 设置命名
	 * 
	 * @param s
	 */
	public void setNaming(String s) {
		this.naming = new Naming(s);
	}

	/**
	 * 返回命名
	 * 
	 * @return
	 */
	public Naming getNaming() {
		return this.naming;
	}

	/**
	 * 返回命名字符串
	 * 
	 * @return
	 */
	public String getNamingString() {
		if (naming == null) {
			return null;
		}
		return naming.toString();
	}
}
