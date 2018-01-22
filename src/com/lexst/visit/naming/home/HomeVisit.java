/**
 *
 */
package com.lexst.visit.naming.home;

import com.lexst.site.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;

public interface HomeVisit extends Visit {

	/**
	 * get server current time
	 * @return
	 * @throws VisitException
	 */
	long currentTime() throws VisitException;

	/**
	 * handshake time
	 * @param type	(site type)
	 * @return
	 * @throws VisitException
	 */
	int getSiteTimeout(int type) throws VisitException;

	/**
	 * notify home site, site is active
	 * @param type	(site type)
	 * @param host	(host address)
	 * @return
	 * @throws VisitException
	 */
	int hello(int type, SiteHost host) throws VisitException;

	/**
	 * @return
	 * @throws VisitException
	 */
	Site[] batchWorkSite() throws VisitException;
	
	/**
	 * @return
	 * @throws VisitException
	 */
	Site[] batchDataSite() throws VisitException;
	
	/**
	 * request a log site by target site, include: home, work, data, call
	 * @param type	(site type)
	 * @return log site address
	 * @throws VisitException
	 */
	SiteHost findLogSite(int type) throws VisitException;
	
	/**
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findDataSite(String schema, String table) throws VisitException;
	
	/**
	 * @param schema
	 * @param table
	 * @param rank
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findDataSite(String schema, String table, int rank) throws VisitException;

	/**
	 * @param schema
	 * @param table
	 * @param chunkid
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findDataSite(String schema, String table, long chunkid) throws VisitException;
	
	/**
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findCallSite(String schema, String table) throws VisitException;

	/**
	 * @param naming
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findCallSite(String naming) throws VisitException;

	/**
	 * @param naming
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findCallSite(String naming, String schema, String table) throws VisitException;
	
	/**
	 * find work site by naming
	 * @param naming
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findWorkSite(String naming) throws VisitException;

	/**
	 * find build site by space
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findBuildSite(String schema, String table) throws VisitException;
	
	/**
	 * find build site by naming
	 * @param naming
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findBuildSite(String naming) throws VisitException;

	/**
	 * get all build site
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] findBuildSite() throws VisitException;
	
	/**
	 * request chunk id, only data site
	 * @param num
	 * @return
	 * @throws VisitException
	 */
	long[] pullSingle(int num) throws VisitException;
	
	/**
	 * request table pid
	 * @param schema
	 * @param table
	 * @param num
	 * @return
	 * @throws VisitException
	 */
	Number[] pullKey(String schema, String table, int num) throws VisitException;

	/**
	 * apply table space, call site request
	 * @return
	 * @throws VisitException
	 */
	Space[] balance(int num) throws VisitException;
	
	/**
	 * apply chunk size 's table
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	int findChunkSize(String schema, String table) throws VisitException;

	/**
	 * login a site (include: log site, call site, data site, query site)
	 * @param site
	 * @return
	 * @throws VisitException
	 */
	boolean login(Site site) throws VisitException;

	/**
	 * logout site
	 * @param type
	 * @param local
	 * @return
	 * @throws VisitException
	 */
	boolean logout(int type, SiteHost local) throws VisitException;

	/**
	 * 重新登录到HOME节点
	 * @param site
	 * @return
	 * @throws VisitException
	 */
	boolean relogin(Site site) throws VisitException;

	/**
	 * 查找数据库表配置
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	Table findTable(String schema, String table) throws VisitException;
	
	/**
	 * 建立数据库表实际存储空间
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean createSpace(Table table) throws VisitException;
	
	/**
	 * 删除数据库表实际存储空间
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean deleteSpace(String schema, String table) throws VisitException;
	
	/**
	 * 重构指定表，指定重构的键和DATA主机地址
	 * 
	 * @param schema
	 * @param table
	 * @param columnId
	 * @param hosts
	 * @return
	 * @throws VisitException
	 */
	Address[] rebuild(String schema, String table, short columnId, Address[] hosts) throws VisitException;
	
	/**
	 * load index to memory
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	Address[] loadIndex(String schema, String table, Address[] hosts) throws VisitException;
	
	/**
	 * release index from memory
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	Address[] stopIndex(String schema, String table, Address[] hosts) throws VisitException;
	
	/**
	 * load chunk to memory
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	Address[] loadChunk(String schema, String table, Address[] hosts) throws VisitException;
	
	/**
	 * release chunk from memory
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	Address[] stopChunk(String schema, String table, Address[] hosts) throws VisitException;
	
	/**
	 * build task to build site
	 * @param naming
	 * @param hosts
	 * @return
	 * @throws VisitException
	 */
	Address[] buildTask(String naming, Address[] hosts) throws VisitException;
	
	/**
	 * query home site, allow download 
	 * @param chunkId
	 * @param local
	 * @return
	 * @throws VisitException
	 */
	boolean agree(long chunkId, SiteHost local) throws VisitException;
	
	/**
	 * publish a new chunk
	 * @param local
	 * @param schema
	 * @param table
	 * @param chunkId
	 * @param length
	 * @return
	 * @throws VisitException
	 */
	boolean publish(SiteHost local, String schema, String table, long chunkId, long length) throws VisitException;

	/**
	 * prime site send to home site, upgrade chunk
	 * @param local
	 * @param schema
	 * @param table
	 * @param oldIds
	 * @param newIds
	 * @return
	 * @throws VisitException
	 */
	boolean upgrade(SiteHost local, String schema, String table, long[] oldIds, long[] newIds) throws VisitException;

	/**
	 * query home site(build pool), allow download from build site
	 * @param schema
	 * @param table
	 * @param chunkid
	 * @param length
	 * @param modified
	 * @return
	 * @throws VisitException
	 */
	boolean	accede(String schema, String table, long chunkid, long length, long modified) throws VisitException;
	
	/**
	 * @param naming
	 * @return
	 * @throws VisitException
	 */
	long[] findBuildChunk(String naming) throws VisitException;
	
	/**
	 * @param schema
	 * @param siteType
	 * @param sites
	 * @return
	 * @throws VisitException
	 */
	long showChunkSize(String schema, int siteType, Address[] sites) throws VisitException;

	/**
	 * @param schema
	 * @param table
	 * @param siteType
	 * @param sites
	 * @return
	 * @throws VisitException
	 */
	long showChunkSize(String schema, String table, int siteType, Address[] sites) throws VisitException;

	/**
	 * show task naming and ip address
	 * @param tag
	 * @param sites
	 * @return
	 * @throws VisitException
	 */
	String showTask(String tag, Address[] sites) throws VisitException;
	
	/**
	 * show match address
	 * @param site (site type, all, home, log, data, work, build, call)
	 * @return
	 * @throws VisitException
	 */
	SiteHost[] showSite(int site) throws VisitException;
	
	/**
	 * send code point stream to home site
	 * @param compress
	 * @param stream
	 * @return
	 * @throws VisitException
	 */
	boolean sendCodePoints(String compress, byte[] stream) throws VisitException;
	
	/**
	 * find code point record
	 * @param schema
	 * @param table
	 * @param columnId
	 * @return
	 * @throws VisitException
	 */
	int[] findCodePoints(String schema, String table, short columnId) throws VisitException;
}