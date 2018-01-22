/**
 *
 */
package com.lexst.site.log;

import java.io.*;

import com.lexst.site.*;

/**
 * 日志节点针对其它节点的监听分类
 * 
 */
public class LogNode implements Serializable, Cloneable, Comparable<LogNode> {

	private static final long serialVersionUID = 6612766909264581042L;

	/** 日志节点族 **/
	private int family;

	/** 监听端口 **/
	private int port;

	/**
	 * 初始化并且设置节点参数
	 * 
	 * @param family
	 * @param port
	 */
	public LogNode(int family, int port) {
		super();
		this.setFamily(family);
		this.setPort(port);
	}

	/**
	 * 初始化并且复制日志节点参数
	 * 
	 * @param node
	 */
	public LogNode(LogNode node) {
		super();
		this.setFamily(node.family);
		this.setPort(node.port);
	}

	/**
	 * 设置节点族
	 * 
	 * @param i
	 */
	public void setFamily(int i) {
		this.family = i;
	}

	/**
	 * 返回节点族
	 * 
	 * @return
	 */
	public int getFamily() {
		return this.family;
	}

	/**
	 * 设置监听端口
	 * 
	 * @param i
	 */
	public void setPort(int i) {
		this.port = i;
	}

	/**
	 * 返回监听端口
	 * 
	 * @return
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * 监听节点标记
	 * @return
	 */
	public String getTag() {
		String tag = null;
		switch (family) {
		case Site.TOP_SITE:
			tag = "top";
			break;
		case Site.HOME_SITE:
			tag = "home";
			break;
		case Site.DATA_SITE:
			tag = "data";
			break;
		case Site.CALL_SITE:
			tag = "call";
			break;
		case Site.WORK_SITE:
			tag = "work";
			break;
		case Site.BUILD_SITE:
			tag = "build";
			break;
		}
		return tag;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != LogNode.class) {
			return false;
		} else if (object == this) {
			return true;
		}

		return this.compareTo((LogNode) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return family ^ port;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new LogNode(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(LogNode node) {
		int ret = (family < node.family ? -1 : (family > node.family ? 1 : 0));
		if (ret == 0) {
			ret = (port < node.port ? -1 : (port > node.port ? 1 : 0));
		}
		return ret;
	}

}