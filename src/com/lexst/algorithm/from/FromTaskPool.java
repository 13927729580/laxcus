/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * diffuse task manager ( "from" command )
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 10/21/2010
 * 
 * @see com.lexst.algorithm.from
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.algorithm.from;

import com.lexst.algorithm.*;
import com.lexst.util.naming.*;

public class FromTaskPool extends TableTaskPool {

	private static FromTaskPool selfHandle = new FromTaskPool();

	/**
	 * default function
	 */
	private FromTaskPool() {
		super();
	}

	/**
	 * 返回静态句柄
	 * 
	 * @return
	 */
	public static FromTaskPool getInstance() {
		return FromTaskPool.selfHandle;
	}

	/**
	 * 根据命名查找对应的diffuse命名实例
	 * 
	 * @param naming
	 * @return
	 */
	public FromTask find(Naming naming) {
		FromTask task = (FromTask) super.findTask(naming);
		// 给FromTask设置所有人句柄,提供数据库表检索服务
		if (task != null) {
			task.setParent(this);
		}
		return task;
	}

	/**
	 * 根据命名查找对应的命名
	 * 
	 * @param naming
	 * @return
	 */
	public FromTask find(String naming) {
		return this.find(new Naming(naming));
	}

}