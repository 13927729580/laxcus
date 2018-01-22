/**
 *
 */
package com.lexst.visit.naming.call;

import com.lexst.site.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;

public interface CallVisit extends Visit {

	/**
	 * 建立数据库表
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean createSpace(Table table) throws VisitException;

	/**
	 * 删除数据库表
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean deleteSpace(String schema, String table) throws VisitException;

	/**
	 * 写入一条记录
	 * @param object
	 * @param sync
	 * @return
	 * @throws VisitException
	 */
	int insert(Insert object, boolean sync) throws VisitException;

	/**
	 * 写入一组记录
	 * @param object
	 * @param sync
	 * @return
	 * @throws VisitException
	 */
	int inject(Inject object, boolean sync) throws VisitException;

	/**
	 * SQL检索数据
	 * @param object
	 * @return
	 * @throws VisitException
	 */
	byte[] select(Select object) throws VisitException;

	/**
	 * 并行分布计算
	 * @param object
	 * @return
	 * @throws VisitException
	 */
	byte[] conduct(Conduct object) throws VisitException;
	
	/**
	 * SQL删除操作
	 * @param object
	 * @return
	 * @throws VisitException
	 */
	long delete(Delete object) throws VisitException;

	/**
	 * SQL更新数据
	 * @param object
	 * @return
	 * @throws VisitException
	 */
	long update(Update object) throws VisitException;
	
	/**
	 * 登录到CALL节点
	 * @param site
	 * @return
	 * @throws VisitException
	 */
	boolean login(Site site) throws VisitException;
	
	/**
	 * 录销登录，从CALL节点
	 * @param type
	 * @param local
	 * @return
	 * @throws VisitException
	 */
	boolean logout(int type, SiteHost local) throws VisitException;
	
	/**
	 * 重新登陆到CALL节点
	 * @param site
	 * @return
	 * @throws VisitException
	 */
	boolean relogin(Site site) throws VisitException;

	/**
	 * 与CALL节点通信，保持激活状态
	 * @param type
	 * @param local
	 * @return
	 * @throws VisitException
	 */
	boolean refresh(int type, SiteHost local) throws VisitException;
}