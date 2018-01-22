/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.choose;

import java.util.*;

import com.lexst.sql.charset.codepoint.*;
import com.lexst.sql.chunk.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;

/**
 * 检索DATA节点资源的配置接口。<br>
 * 
 * 接口实现包括CALL、WORK节点。<br>
 *
 */
public interface FromChooser {
	
	/**
	 * 增加一个数据库表
	 * @param table
	 * @return
	 */
	boolean addTable(Table table);
	
	/**
	 * 删除一个数据库表
	 * @param space
	 * @return
	 */
	boolean removeTable(Space space);

	/**
	 * 根据数据表名，查找对应的数据表配置
	 * @param space
	 * @return
	 */
	Table findTable(Space space);
	
	/**
	 * 根据数据表名，查找数据分布范围
	 * @param space
	 * @return
	 */
	IndexModule findIndexModule(Space space);

	/**
	 * 根据表区间名，查找代码位区域
	 * @param docket
	 * @return
	 */
	CodeIndexModule findCodeIndexModule(Docket docket);

	/**
	 * 根据命名，找到对应的data节点地址集合
	 * @param naming
	 * @return
	 */
	SiteSet findFromSites(Naming naming);
	
	/**
	 * 根据命名和数据库表，找到对应的data节点地址集合
	 * 
	 * @param naming
	 * @param space
	 * @return
	 */
	SiteSet findFromSites(Naming naming, Space space);
	
	/**
	 * 根据chunkid，查找对应的data节点集合
	 * @param chunkid
	 * @return
	 */
	SiteSet findFromSites(long chunkid);
	
	/**
	 * 根据数据表名，找到对应的主节点集合
	 * @param space
	 * @return
	 */
	SiteSet findPrime(Space space);

	/**
	 * 返回当前的全部的diffuse命名
	 * @return
	 */
	List<Naming> getNamings();
	
	/**
	 * 返回当前全部的数据库表名
	 * @return
	 */
	List<Space> getSpaces();
}
