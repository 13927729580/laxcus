/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * aggregate task manager ("to" command)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 10/23/2010
 * 
 * @see com.lexst.algorithm.to
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.algorithm.to;

import com.lexst.algorithm.*;
import com.lexst.algorithm.choose.*;
import com.lexst.util.naming.*;

public class ToTaskPool extends TaskPool {

	/** aggregate计算池静态句柄 **/
	private static ToTaskPool selfHandle = new ToTaskPool();

	/** data节点资源配置器 */
	private FromChooser chooser;
	
	/**
	 * default function
	 */
	private ToTaskPool() {
		super();
	}

	/**
	 * 返回静态句柄(在环境中唯一)
	 * 
	 * @return
	 */
	public static ToTaskPool getInstance() {
		return ToTaskPool.selfHandle;
	}

	/**
	 * 设置DATA节点资源配置器
	 * 
	 * @param s
	 */
	public void setFromChooser(FromChooser s) {
		this.chooser = s;
	}

	/**
	 * 返回DATA节点资源配置器
	 * 
	 * @return
	 */
	public FromChooser getFromChooser() {
		return this.chooser;
	}

	/**
	 * 根据任务命名，查找对应的"aggregate"阶段任务实例
	 * 
	 * @param naming
	 * @return
	 */
	public ToTask find(Naming naming) {
		ToTask task = (ToTask) super.findTask(naming);
		if (task != null) {
			task.setFromChooser(this.chooser);
		}
		return task;
	}

	/**
	 * 根据任务命名，查找对应的"aggregate"阶段任务实例
	 * 
	 * @param naming
	 * @return
	 */
	public ToTask find(String naming) {
		return this.find(new Naming(naming));
	}
}