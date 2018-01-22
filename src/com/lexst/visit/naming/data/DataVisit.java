/**
 *
 */
package com.lexst.visit.naming.data;

import com.lexst.sql.chunk.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;

public interface DataVisit extends Visit {
	
	/**
	 * apply data site's rank
	 * @return
	 * @throws VisitException
	 */
	int applyRank() throws VisitException;

	/**
	 * find chunk information by space
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	ChunkStatus[] findChunk(String schema, String table) throws VisitException;
	
	/**
	 * send message to data site(slive node), download a chunk
	 * @param host
	 * @param schema
	 * @param table
	 * @param chunkid
	 * @param length
	 * @return
	 * @throws VisitException
	 */
	boolean distribute(SiteHost host, String schema, String table, long chunkid, long length) throws VisitException;
	
	/**
	 * 发送消息到DATA从节点(slave node)，下载已经重构的数据块(chunk)
	 * @param host
	 * @param schema
	 * @param table
	 * @param oldIds
	 * @param newIds
	 * @return
	 * @throws VisitException
	 */
	boolean upgrade(SiteHost host, String schema, String table, long[] oldIds, long[] newIds) throws VisitException;

	/**
	 * notify data site, check and update chunk
	 * @param schema
	 * @param table
	 * @param host
	 * @return
	 * @throws VisitException
	 */
	boolean revive(String schema, String table, SiteHost from) throws VisitException;

	/**
	 * request a index set
	 * @param schema
	 * @param table
	 * @param pwd
	 * @return
	 * @throws VisitException
	 */
	IndexTable findIndex(String schema, String table) throws VisitException;

	/**
	 * create data space
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean createSpace(Table table) throws VisitException;

	/**
	 * remove data space
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean deleteSpace(String schema, String table) throws VisitException;
	
	/**
	 * 重构DATA节点上的表数据
	 * 
	 * @param scheme - 数据库名
	 * @param table -  数据库下的表名
	 * @param columnId - 指定表的列ID(以此列ID为准进行重构，默认是0时将以原表主键重构)
	 * @return
	 * @throws VisitException
	 */
	public boolean rebuild(String scheme, String table, short columnId) throws VisitException;
	
	/**
	 * load index to memory
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean loadIndex(String schema, String table) throws VisitException;
	
	/**
	 * release index from memory
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean stopIndex(String schema, String table) throws VisitException;
	
	/**
	 * load chunk into memory
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean loadChunk(String schema, String table) throws VisitException;
	
	/**
	 * clear chunk from memory
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean stopChunk(String schema, String table) throws VisitException;
	
	/**
	 * count chunk-size by schema
	 * @param schema
	 * @return
	 * @throws VisitException
	 */
	long showChunkSize(String schema) throws VisitException;
	
	/**
	 * count chunk-size by space
	 * @param schema
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	long showChunkSize(String schema, String table) throws VisitException;
	
	/**
	 * conduct状态下，根据元信息提供数据
	 * @param jobid
	 * @param mod
	 * @param begin
	 * @param end
	 * @return
	 */
	byte[] suckup(long jobid, int mod, long begin, long end) throws VisitException;
}
