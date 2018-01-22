/**
 *
 */
package com.lexst.algorithm.to;

import com.lexst.algorithm.*;
import com.lexst.algorithm.choose.*;
import com.lexst.algorithm.disk.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;

/**
 * 并行分布计算在"aggregate"阶段的任务分配接口。<br>
 */
public abstract class ToTask extends BasicTask {

	/** 外部配置调用接口 */
	private FromChooser chooser;

	/** "aggregate"阶段任务配置 **/
	private ToPhase phase;

	/**
	 * default structor
	 */
	public ToTask() {
		super();
	}

	/**
	 * 设置data节点配置接口(ToTaskPool赋值，只限内部使用)
	 * 
	 * @param s
	 */
	protected void setFromChooser(FromChooser s) {
		this.chooser = s;
	}

	/**
	 * 返回调用接口
	 * 
	 * @return
	 */
	protected FromChooser getFromChooser() {
		return this.chooser;
	}

	/**
	 * 设置ToPhase实例(在ToTask生成后设置)
	 * 
	 * @param phase
	 */
	public void setToPhase(ToPhase phase) {
		this.phase = phase;
	}

	/**
	 * 返回ToPhase实例
	 * 
	 * @return
	 */
	public ToPhase getToPhase() {
		return this.phase;
	}

	/**
	 * 加入一段分片数据(分片信息和数据流组成，数据来自DATA节点或者上一级WORK节点)
	 * 
	 * @param field
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public abstract boolean inject(DiskField field, byte[] b, int off, int len)
			throws ToTaskException;

	/**
	 * 完成aggregate阶段任务
	 * 
	 * @param trustor
	 *            - 数据的磁盘委托器(如果向磁盘写入数据时需要)
	 * @return
	 */
	public abstract byte[] complete(DiskTrustor trustor) throws ToTaskException;

}