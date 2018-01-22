/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct;

/**
 * 数据/资料键入器。<br>
 * 用户在终端输入或者初始化之前阶段的参数设置。<br>
 * 
 */
public abstract class InputObject extends NamingObject {

	private static final long serialVersionUID = 1705414778924149840L;

	/** 指定的DATA/WORK节点数，默认是0不指定。此参数一般在命名时定义 **/
	private int sites;

	/**
	 * default
	 */
	protected InputObject() {
		super();
		this.sites = 0;
	}

	/**
	 * @param in
	 */
	protected InputObject(InputObject in) {
		this();
		this.setSites(in.sites);
	}

	/**
	 * 指定DATA/WORK节点数
	 * 
	 * @param i
	 */
	public void setSites(int i) {
		this.sites = i;
	}

	/**
	 * 返回DATA节点数
	 * 
	 * @return
	 */
	public int getSites() {
		return this.sites;
	}

}
