/**
 * @author admin@laxcus.com
 */
package com.lexst.algorithm.build;

import com.lexst.site.build.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;

/**
 * 数据重构选择器
 *
 */
public interface BuildChooser {

	/**
	 * 通知宿主管理器，任务完成重新注册
	 * 
	 * @param f
	 */
	void setLogin(boolean f);

	/**
	 * 返回HOME节点地址
	 * 
	 * @return
	 */
	SiteHost getHome();

	/**
	 * 取BUILD节点资源
	 * 
	 * @return
	 */
	BuildSite getLocal();

	/**
	 * 从管理队列中删除运行任务命名
	 * 
	 * @param naming
	 * @return
	 */
	boolean removeTask(Naming naming);
}