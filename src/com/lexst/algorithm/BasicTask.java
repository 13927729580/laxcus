/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * distribute task (basic class)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 10/19/2010
 * 
 * @see com.lexst.algorithm
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.algorithm;

/**
 * 热发布任务实例基础类 <br>
 * 
 * 项目配置参数格式： <br>
 * 
 * <task>
 * 	<naming> org_xxx_naming_object_xxx </naming>
 *  <class> org.xxx.xxx.EngineTask </class>
 *  <spaces> schema1.table1 , schema2.table2, schema3.table3 </spaces>
 *  <resource> schema or table or filename or other </resource> 
 * 	<project-class> org.xxx.xxx.EngineProject </project-class> 
 * </task>
 * 
 * <br><br>
 * naming必须保证在整个环境中唯一，否则会引起命名混乱。格式由ASCII码的"字母、数字、下划线"组成，其它非法。<br>
 * class是任务实现类，必须从BasicTask类派生。<br>
 * spaces 是用户配置的数据库表名集合，表名之间用逗号分隔，如schema1.table1, schema2.table2, 
 * resource 是用户自定义的资源(文本格式)，具体由用户去解析。一般有用户定义的参数、硬盘文件等。<br>
 * project-class 是项目实现类，必须从Project类派生。<br>
 */
public class BasicTask {

	/** 任务所属项目，在TaskPool生成任务实例时指派 **/
	private Project project;
	
	/**
	 * 
	 */
	public BasicTask() {
		super();
	}

	/**
	 * set task project
	 * 
	 * @param s
	 */
	public void setProject(Project s) {
		this.project = s;
	}

	/**
	 * get task project
	 * 
	 * @return
	 */
	public Project getProject() {
		return this.project;
	}

	/**
	 * thread wait
	 * 
	 * @param timeout
	 */
	protected synchronized void delay(long timeout) {
		try {
			super.wait(timeout);
		} catch (InterruptedException exp) {

		}
	}

	/**
	 * notify thread
	 */
	protected synchronized void wakeup() {
		try {
			super.notify();
		} catch (IllegalMonitorStateException exp) {

		}
	}
}