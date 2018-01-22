/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.parse;

import com.lexst.sql.schema.*;

/**
 * SQL配置资源检查接口
 * 
 */
public interface SQLChooser {

	/**
	 * 查找一个数据库表
	 * @param space
	 * @return
	 */
	Table findTable(Space space);

	/**
	 * 检查数据库表是否存在
	 * @param space
	 * @return
	 */
	boolean onTable(Space space);

	/**
	 * 检查数据库是否存在
	 * @param schema
	 * @return
	 */
	boolean onSchema(String schema);
	
	/**
	 * 检查账号是否存在
	 * @param username
	 * @return
	 */
	boolean onUser(String username);
}
