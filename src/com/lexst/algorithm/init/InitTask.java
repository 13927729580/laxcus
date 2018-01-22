/**
 * @email admin@lexst.com
 *
 */
package com.lexst.algorithm.init;

import com.lexst.algorithm.*;
import com.lexst.algorithm.choose.*;
import com.lexst.sql.statement.*;

/**
 * conduct任务启动前的初始化类，为后续调用分配参数。<br>
 * 此接口位于CALL节点，在conduct运行前调用。<br>
 */
public abstract class InitTask extends BasicTask {

	/** DATA节点资源选择器(只限子类内部使用) **/
	private FromChooser fromChooser;

	/** WORK节点资源选择器(只限子类内部使用) **/
	private ToChooser toChooser;

	/**
	 * default
	 */
	public InitTask() {
		super();
	}

	/**
	 * 设置DATA节点资源配置器
	 * 
	 * @param s
	 */
	protected void setFromChooser(FromChooser s) {
		this.fromChooser = s;
	}

	/**
	 * 返回DATA节点资源配置器
	 * 
	 * @return
	 */
	protected FromChooser getFromChooser() {
		return this.fromChooser;
	}

	/**
	 * 设置WORK节点资源配置器
	 * 
	 * @param s
	 */
	protected void setToChooser(ToChooser s) {
		this.toChooser = s;
	}

	/**
	 * 返回WORK节点资源配置器
	 * 
	 * @return
	 */
	protected ToChooser getToChooser() {
		return this.toChooser;
	}

	/**
	 * 分配计算资源和定义规则
	 * 
	 * @param conduct
	 * @return
	 */
	public abstract Conduct init(Conduct conduct) throws InitTaskException;

}