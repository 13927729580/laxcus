/**
 * 
 */
package com.lexst.algorithm.build;

import com.lexst.algorithm.*;

/**
 * marshal/educe算法实现，数据重构接口，属ETL应用范畴<br><br>
 * 
 * 区别Install.rebuild的操作，Install.rebuild做删除过期数据和新的压缩处理，不会改变表的原有结构。<br>
 * BuildTask允许操作范围更大，在完成上述工作任务之外，允许将一个表或者几个表的数据，转换/合并/压缩成一个新表，形成新的数据。<br>
 * 具体实现由用户中间件完成。<br>
 *
 */
public abstract class BuildTask extends BasicTask {

	/** BUILD 节点操作选择器 **/
	private BuildChooser chooser;

	/**
	 * default
	 */
	public BuildTask() {
		super();
	}

	/**
	 * 设置BUILD操作选择器
	 * 
	 * @param s
	 */
	protected void setBuildChooser(BuildChooser s) {
		this.chooser = s;
	}

	/**
	 * 返回BUILD操作选择器
	 * 
	 * @return
	 */
	protected BuildChooser getBuildChooser() {
		return this.chooser;
	}

	/**
	 * 从拥有者队列集合中注册自己
	 * 
	 * @return
	 */
	public boolean release() {
		Project project = super.getProject();
		if (chooser != null && project != null) {
			return chooser.removeTask(project.getTaskNaming());
		}
		return false;
	}

	/**
	 * 启动重构任务，marshal/educe算法实现
	 * 
	 * @return
	 */
	public abstract boolean rebuild() throws BuildTaskException;

	/**
	 * 外部要求停止重构任务
	 */
	public abstract void halt() throws BuildTaskException;

}