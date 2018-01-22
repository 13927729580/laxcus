/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * naming listener
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
 * 任务命名更新监听接口，在各节点上实现。<br>
 */
public interface TaskEventListener {

	/**
	 * 通知接口实现类(通常是launcher)，更新全部任务命名
	 */
	void updateNaming();
	
}
