/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.choose;

import java.util.*;

import com.lexst.util.host.*;
import com.lexst.util.naming.*;

/**
 * 检索WORK节点的资源配置接口。<br>
 * 接口实现在CALL节点上。<br>
 *
 */
public interface ToChooser {

	/**
	 * 根据命名，找到对应的WORK节点地址集合
	 * @param naming
	 * @return
	 */
	SiteSet findToSites(Naming naming);

	/**
	 * 返回当前命名集合
	 * @return
	 */
	List<Naming> getNamings();
}
