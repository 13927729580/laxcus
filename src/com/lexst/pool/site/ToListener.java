/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.pool.site;

/**
 * To存储池监听器。当ToPool发生数据变化时，通知宿主节点。<br>
 *
 */
public interface ToListener {

	/**
	 * 通知宿主节点，关联的WORK节点配置已经改变
	 */
	void updateWorkRecord();
	
}
