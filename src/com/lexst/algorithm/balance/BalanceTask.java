/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * balance task
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

import java.util.*;

import com.lexst.algorithm.*;
import com.lexst.sql.conduct.matrix.*;

/**
 * 
 * 实现数据平均分配的接口类
 *
 */
public abstract class BalanceTask extends BasicTask {

	/**
	 * default
	 */
	public BalanceTask() {
		super();
	}

	
	/**
	 * 根据任务命名和WORK节点的数量，平均分配Area集合
	 * 
	 * @param naming - to命名对象，为平衡分配提供依据
	 * @param sites - work节点总数
	 * @param list - area集合
	 * @return
	 */
	public abstract NetDomain[] split(String naming, int sites, List<DiskArea> list) throws BalanceTaskException;
	
}