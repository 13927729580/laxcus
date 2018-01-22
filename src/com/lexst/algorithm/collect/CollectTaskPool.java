/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * collect task manager ("collect" command)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 11/21/2010
 * 
 * @see com.lexst.algorithm.collect
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.algorithm.collect;

import com.lexst.algorithm.*;
import com.lexst.util.naming.*;


public class CollectTaskPool extends TaskPool {
	
	private static CollectTaskPool selfHandle = new CollectTaskPool();

	/**
	 * default function
	 */
	private CollectTaskPool() {
		super();
	}

	/**
	 * get static handle
	 * @return
	 */
	public static CollectTaskPool getInstance() {
		return CollectTaskPool.selfHandle;
	}

	/**
	 * find a collect task by naming
	 * @param naming
	 * @return
	 */
	public CollectTask find(Naming naming) {
		return (CollectTask) super.findTask(naming);
	}
	
	/**
	 * @param naming
	 * @return
	 */
	public CollectTask find(String naming) {
		return this.find(new Naming(naming));
	}

}