/**
 * 
 */
package com.lexst.algorithm.build;

import com.lexst.algorithm.*;
import com.lexst.util.naming.*;

/**
 * 执行marshal/educe操作的服务管理池
 * 
 */
public class BuildTaskPool extends TableTaskPool {

	/** 静态句柄 **/
	private static BuildTaskPool selfHandle = new BuildTaskPool();

	/** build节点的操作选择器 */
	private BuildChooser chooser;

	/**
	 * 初始化BUILD任务管理池
	 */
	private BuildTaskPool() {
		super();
	}

	/**
	 * 返回BuildTaskPool的静态句柄
	 * 
	 * @return
	 */
	public static BuildTaskPool getInstance() {
		return BuildTaskPool.selfHandle;
	}

	/**
	 * 设置BUILD任务选择器
	 * 
	 * @param s
	 */
	public void setBuildChooser(BuildChooser s) {
		this.chooser = s;
	}

	/**
	 * 返回BUILD任务选择器
	 * 
	 * @return
	 */
	public BuildChooser getBuildChooser() {
		return this.chooser;
	}

	/**
	 * 根据命名，查找Build任务
	 * 
	 * @param naming
	 * @return
	 */
	public BuildTask find(Naming naming) {
		BuildTask task = (BuildTask) super.findTask(naming);
		if (task != null) {
			task.setBuildChooser(this.chooser);
		}
		return task;
	}

	/**
	 * 根据命名，查找Build任务
	 * 
	 * @param naming
	 * @return
	 */
	public BuildTask find(String naming) {
		return this.find(new Naming(naming));
	}
}