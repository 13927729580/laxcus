/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * init task manager
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 12/16/2010
 * 
 * @see com.lexst.algorithm.init
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.algorithm.init;

import com.lexst.algorithm.*;
import com.lexst.algorithm.choose.*;
import com.lexst.util.naming.*;

/**
 * conduct命令初始化任务集合池，在call节点上启动。<br>
 * 
 */
public class InitTaskPool extends TaskPool {

	/** InitTaskPool 静态句柄(一个进程中只允许一个存在) */
	private static InitTaskPool selfHandle = new InitTaskPool();

	/** DATA节点资料选择器 */
	private FromChooser fromChooser;

	/** WORK节点资料选择器 **/
	private ToChooser toChooser;

	/**
	 * default constractor
	 */
	private InitTaskPool() {
		super();
	}

	/**
	 * @return
	 */
	public static InitTaskPool getInstance() {
		return InitTaskPool.selfHandle;
	}

	/**
	 * 设置DATA节点资源配置器
	 * 
	 * @param s
	 */
	public void setFromChooser(FromChooser s) {
		this.fromChooser = s;
	}

	/**
	 * 返回DATA节点资源配置器
	 * 
	 * @return
	 */
	public FromChooser getFromChooser() {
		return this.fromChooser;
	}

	/**
	 * 设置WORK节点资源配置器
	 * 
	 * @param s
	 */
	public void setToChooser(ToChooser s) {
		this.toChooser = s;
	}

	/**
	 * 返回WORK节点资源配置器
	 * 
	 * @return
	 */
	public ToChooser getToChooser() {
		return this.toChooser;
	}

	/**
	 * 根据命名找到匹配的初始化任务
	 * 
	 * @param naming
	 * @return
	 */
	public InitTask find(Naming naming) {
		InitTask task = (InitTask) super.findTask(naming);
		if (task != null) {
			task.setFromChooser(this.fromChooser);
			task.setToChooser(this.toChooser);
		}
		return task;
	}

	/**
	 * 根据命名找到匹配的初始化任务
	 * 
	 * @param naming
	 * @return
	 */
	public InitTask find(String naming) {
		return this.find(new Naming(naming));
	}

}