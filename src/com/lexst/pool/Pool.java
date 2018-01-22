/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * @author scott.liang lexst@126.com
 * @version 1.0 5/2/2009
 * 
 * @see com.lexst.pool
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.pool;

import com.lexst.thread.*;
import com.lexst.util.lock.*;

/**
 * 管理池服务，是所有节点任务池的基础类。包括提供单向/多向锁交替切换的锁服务 <br>
 *
 */
public abstract class Pool extends VirtualThread {

	/** 多向锁(多读单写模式) */
	private MutexLock lock = new MutexLock();

	/**
	 * default
	 */
	protected Pool() {
		super();
		this.setSleep(5);
	}

	/**
	 * 单向锁定
	 * @return
	 */
	protected boolean lockSingle() {
		return lock.lockSingle();
	}

	/**
	 * 解除单向锁定
	 * @return
	 */
	protected boolean unlockSingle() {
		return lock.unlockSingle();
	}

	/**
	 * 多向锁定
	 * @return
	 */
	protected boolean lockMulti() {
		return lock.lockMulti();
	}

	/**
	 * 解除多向锁定
	 * @return
	 */
	protected boolean unlockMulti() {
		return lock.unlockMulti();
	}
	
}