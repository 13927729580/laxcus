/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * balance task manager ("balance" command)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 12/16/2010
 * 
 * @see com.lexst.algorithm.balance
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.algorithm.balance;

import com.lexst.algorithm.*;
import com.lexst.util.naming.*;

/**
 * 数据平均分配任务管理池，用在CALL节点上，只限于CONDUCT命令使用
 * 此调用发生在DIFFUSE之后，AGGREGATE之前
 */
public class BalanceTaskPool extends TaskPool {

	private static BalanceTaskPool selfHandle = new BalanceTaskPool();

	/**
	 * default constractor
	 */
	private BalanceTaskPool() {
		super();
	}

	/**
	 * @return
	 */
	public static BalanceTaskPool getInstance() {
		return BalanceTaskPool.selfHandle;
	}

	/**
	 * find a balance task from naming
	 * @param naming
	 * @return
	 */
	public BalanceTask find(Naming naming) {
		return (BalanceTask)super.findTask(naming);
	}

	/**
	 * find a balance task from naming
	 * @param naming
	 * @return
	 */
	public BalanceTask find(String naming) {
		return this.find(new Naming(naming));
	}
}