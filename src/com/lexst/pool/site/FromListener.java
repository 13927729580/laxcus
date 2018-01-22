/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.pool.site;

/**
 * DATA节点配置监听器
 * 
 */
public interface FromListener {
	
	/**
	 * 通知宿主节点，数据节点记录已经更新
	 */
	void updateDataRecord();

}
