/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2012 lexst.com, All rights reserved
 * 
 * task resource configure
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 2/1/2012
 * 
 * @see com.lexst.util.naming
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.algorithm;

import java.io.*;
import java.util.*;

import com.lexst.sql.schema.*;
import com.lexst.util.naming.*;

/**
 * 并行分布计算在实现节点上的项目配置<br>
 * 具体说明见 BasicTask 和 TaskPool<br>
 * Project保存<task>标签中的naming、class、spaces、resource四项信息，<br>
 * Prorject保存/解释前三项，后一项资源配置由用户的Project继承者类解释<br><br>
 * 
 * 
 * 用户需要实现自己的Project继承类。
 * 
 */
public class Project implements Serializable {

	private static final long serialVersionUID = 3059178580804879586L;

	/** 任务命名 */
	private Naming taskNaming;

	/** 任务类名称 */
	private String taskClass;

	/** 用户定义的数据库表集合 **/
	private Set<Space> spaces = new TreeSet<Space>();

	/** 用户自定义资源数据 */
	protected String resource;

	/**
	 * default
	 */
	public Project() {
		super();
	}

	/**
	 * 设置任务命名
	 * 
	 * @param s
	 */
	public void setTaskNaming(Naming s) {
		this.taskNaming = (Naming) s.clone();
	}

	/**
	 * 返回任务命名
	 * 
	 * @return
	 */
	public Naming getTaskNaming() {
		return this.taskNaming;
	}

	/**
	 * 设置任务类路径
	 * 
	 * @param s
	 */
	public void setTaskClass(String s) {
		this.taskClass = new String(s);
	}

	/**
	 * 返回任务类路径
	 * 
	 * @return
	 */
	public String getTaskClass() {
		return this.taskClass;
	}

	/**
	 * 返回用户自定义配置资源
	 * 
	 * @return
	 */
	public String getResource() {
		return this.resource;
	}

	/**
	 * 设置用户资源(子类声明同名方法，覆盖本方法解析数据)
	 * 
	 * @param s
	 */
	public void setResource(String s) {
		this.resource = new String(s);
	}

	/**
	 * 返回项目中定义的数据库表名集合
	 * 
	 * @return
	 */
	public Set<Space> getSpaces() {
		return this.spaces;
	}

	/**
	 * 解析并且保存数据库表名(在spaces中定义，数据库表名之间由逗号分隔)
	 * 
	 * @param input
	 * @throws SpaceFormatException
	 */
	public void setSpaces(String input) throws SpaceFormatException {
		if (input == null || input.trim().isEmpty()) {
			return;
		}
		String[] items = input.split(",");
		for (String item : items) {
			this.spaces.add(new Space(item));
		}
	}

}