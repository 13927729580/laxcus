/**
 *
 */
package com.lexst.visit.naming.work;

import com.lexst.visit.*;

/**
 * WORK节点RPC访问接口。在WORK节点上实现，由其它节点调用。<br>
 *
 */
public interface WorkVisit extends Visit {

	/**
	 * 根据指定参数，提出WORK节点存储的数据
	 * 
	 * @param jobid
	 * @param mod
	 * @param begin
	 * @param end
	 * @return
	 * @throws VisitException
	 */
	byte[] suckup(long jobid, int mod, long begin, long end) throws VisitException;
}