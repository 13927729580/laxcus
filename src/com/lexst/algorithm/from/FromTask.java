/**
 * 
 */
package com.lexst.algorithm.from;

import com.lexst.algorithm.*;
import com.lexst.algorithm.disk.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;

/**
 * 并行分布计算在"diffuse"阶段的任务分派接口。<br>
 */
public abstract class FromTask extends BasicTask {

	/** From命名任务池 */
	private FromTaskPool parent;

	/**
	 * default
	 */
	public FromTask() {
		super();
	}

	/**
	 * 设置FROM命名任务管理器
	 * 
	 * @param s
	 */
	public void setParent(FromTaskPool s) {
		this.parent = s;
	}

	/**
	 * 返回FROM命名任务管理器(只供内部使用)
	 * 
	 * @return
	 */
	protected FromTaskPool getParent() {
		return this.parent;
	}

	/**
	 * 按照规则重组数据位置，数据写入磁盘，返回分块信息
	 * 
	 * @param trustor
	 * @param phase
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public abstract DiskArea divideup(DiskTrustor trustor, FromPhase phase,
			byte[] b, int off, int len) throws FromTaskException;
}