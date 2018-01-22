/**
 *
 */
package com.lexst.visit.naming.top;

import com.lexst.site.*;
import com.lexst.sql.account.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;

public interface TopVisit extends Visit {

	/**
	 * TOP节点当前时间
	 * @return
	 * @throws VisitException
	 */
	long currentTime() throws VisitException;

	/**
	 * 返回指定类型节点超时时间
	 * @param family	(site family)
	 * @return
	 * @throws VisitException
	 */
	int getSiteTimeout(int family) throws VisitException;

	/**
	 * 激活注册在TOP节点的地址，保持连接状态
	 * @param family	(site family)
	 * @param local		(login site address)
	 * @return
	 * @throws VisitException
	 */
	int hello(int family, SiteHost local) throws VisitException;

	/**
	 * 注册到TOP节点(只限HOME、LIVE节点)
	 * @param site
	 * @return
	 * @throws VisitException
	 */
	boolean login(Site site) throws VisitException;

	/**
	 * 从TOP节点上注册本地节点
	 * @param family
	 * @param local
	 * @return
	 * @throws VisitException
	 */
	boolean logout(int family, SiteHost local) throws VisitException;

	/**
	 * 重新注册到TOP节点(只限HOME、LIVE节点)
	 * @param site
	 * @return
	 * @throws VisitException
	 */
	boolean relogin(Site site) throws VisitException;
	
	/**
	 * 申请数据块标识号(chunk identity)。HOME节点申请，再分派给DATA节点
	 * @param num
	 * @return
	 * @throws VisitException
	 */
	long[] applyChunkId(int num) throws VisitException;

	/**
	 * apply table prime key
	 * @param db
	 * @param table
	 * @param num
	 * @return
	 * @throws VisitException
	 */
	Number[] pullKey(String db, String table, int num) throws VisitException;

	/**
	 * @param db
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	int findChunkSize(String db, String table) throws VisitException;

	/**
	 * set chunk file size
	 * @param db
	 * @param table
	 * @param size
	 * @return
	 * @throws VisitException
	 */
	boolean setChunkSize(String db, String table, int size) throws VisitException;

	/**
	 * find home site by space
	 * @param db
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findHomeSite(String db, String table) throws VisitException;

	/**
	 * @param naming
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] selectCallSite(String naming) throws VisitException;

	/**
	 * @param db
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] selectCallSite(String db, String table) throws VisitException;
	
	/**
	 * @param naming
	 * @param db
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] selectCallSite(String naming, String db, String table) throws VisitException;
	
	/**
	 * 建立数据库
	 * @param local (live site address)
	 * @param schema
	 * @return
	 * @throws VisitException
	 */
	boolean createSchema(SiteHost local, Schema schema) throws VisitException;

	/**
	 * 查找数据库
	 * @param local
	 * @param schema
	 * @return
	 * @throws VisitException
	 */
	Schema findSchema(SiteHost local, String schema) throws VisitException;

	/**
	 * delete a database
	 * @param local
	 * @param db
	 * @return
	 * @throws VisitException
	 */
	boolean deleteSchema(SiteHost local, String db) throws VisitException;

	/**
	 * get all database name
	 * @param local
	 * @return
	 * @throws VisitException
	 */
	String[] getSchemas(SiteHost local) throws VisitException;

	/**
	 * 建立一个普通用户账号
	 * @param local
	 * @param user
	 * @return
	 * @throws VisitException
	 */
	boolean createUser(SiteHost local, User user) throws VisitException;

	/**
	 * 删除一个普通用户账号
	 * @param local
	 * @param username
	 * @return
	 * @throws VisitException
	 */
	boolean deleteUser(SiteHost local, String username) throws VisitException;

	/**
	 * 修改账号信息
	 * @param local - 注册节点绑定的本地地址
	 * @param user - 用户账号
	 * @return
	 * @throws VisitException
	 */
	boolean alterUser(SiteHost local, User user) throws VisitException;
	
	/**
	 * 检查一个账号用户名是否存在
	 * @param local - 当前注册节点地址
	 * @param username - SHA1字符串（16进制)
	 * @return
	 * @throws VisitException
	 */
	boolean onUser(SiteHost local, String username) throws VisitException;

	/**
	 * add a user permit
	 * @param local
	 * @param permit
	 * @return
	 * @throws VisitException
	 */
	boolean addPermit(SiteHost local, Permit permit) throws VisitException;

	/**
	 * delete a user permit
	 * @param local
	 * @param permit
	 * @return
	 * @throws VisitException
	 */
	boolean deletePermit(SiteHost local, Permit permit) throws VisitException;

	/**
	 * get user permit
	 * @param local
	 * @return
	 * @throws VisitException
	 */
	Permit[] getPermits(SiteHost local) throws VisitException;

	/**
	 * create a table to top site
	 * @param local
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean createTable(SiteHost local, Table table) throws VisitException;

	/**
	 * 根据表名找到对应的数据库表配置
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	Table findTable(SiteHost local, String schema, String table) throws VisitException;

	/**
	 * 删除数据库表
	 * @param local
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean deleteTable(SiteHost local, String schema, String table) throws VisitException;

	/**
	 * 返回当前账号下的所有数据库表配置
	 * @param local
	 * @return
	 * @throws VisitException
	 */
	Table[] getTables(SiteHost local) throws VisitException;

	/**
	 * 检查LIVE登录账号用户类型，是管理员账号或者普通用户
	 * @param local
	 * @return
	 * @throws VisitException
	 */
	short checkIdentified(SiteHost local) throws VisitException;

	/**
	 * 定义表的数据重构时间
	 * @param schema
	 * @param table
	 * @param columnId
	 * @param type
	 * @param time
	 * @return
	 * @throws VisitException
	 */
	boolean setRebuildTime(String schema, String table, short columnId, int type, long time) throws VisitException;

	/**
	 * 重构某个表
	 * @param local
	 * @param schema
	 * @param table
	 * @param columnId
	 * @param addresses
	 * @return
	 * @throws VisitException
	 */
	Address[] rebuild(SiteHost local, String schema, String table, short columnId, Address[] addresses) throws VisitException;

	/**
	 * load chunk index to memory
	 * @param local
	 * @param schema
	 * @param table
	 * @param addresses
	 * @return
	 * @throws VisitException
	 */
	Address[] loadIndex(SiteHost local, String schema, String table, Address[] addresses) throws VisitException;

	/**
	 * release chunk index
	 * @param local
	 * @param schema
	 * @param table
	 * @param addresses
	 * @return
	 * @throws VisitException
	 */
	Address[] stopIndex(SiteHost local, String schema, String table, Address[] addresses) throws VisitException;

	/**
	 * load chunk data
	 * @param local
	 * @param schema
	 * @param table
	 * @param addresses
	 * @return
	 * @throws VisitException
	 */
	Address[] loadChunk(SiteHost local, String schema, String table, Address[] addresses) throws VisitException;

	/**
	 * release chunk data
	 * @param local
	 * @param schema
	 * @param table
	 * @param addresses
	 * @return
	 * @throws VisitException
	 */
	Address[] stopChunk(SiteHost local, String schema, String table, Address[] addresses) throws VisitException;

	/**
	 * build a task to build site
	 * @param local
	 * @param naming
	 * @param addresses
	 * @return
	 * @throws VisitException
	 */
	Address[] buildTask(SiteHost local, String naming, Address[] addresses) throws VisitException;

	/**
	 * @param local
	 * @param schema
	 * @param siteType
	 * @param sites
	 * @return
	 * @throws VisitException
	 */
	long showChunkSize(SiteHost local, String schema, int siteType, Address[] sites) throws VisitException;

	/**
	 * @param local
	 * @param schema
	 * @param table
	 * @param siteType
	 * @param sites
	 * @return
	 * @throws VisitException
	 */
	long showChunkSize(SiteHost local, String schema, String table, int siteType, Address[] sites) throws VisitException;

	/**
	 * @param local
	 * @param tag (keyword: all, diffuse, aggregate, build)
	 * @param address
	 * @return
	 * @throws VisitException
	 */
	String showTask(SiteHost local, String tag, Address[] address) throws VisitException;

	/**
	 * show site address
	 * @param site
	 * @param from
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] showSite(int site, Address[] froms) throws VisitException;
}