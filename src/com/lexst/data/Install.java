/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * lexst database interface
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 12/11/2009
 * 
 * @see com.lexst.data
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.data;

public final class Install {

	public static boolean loaded = false;

	static {
		try {
			System.loadLibrary("lexstdb");
			Install.loaded = true;
		} catch (Throwable exp) {
			exp.printStackTrace();
		}
	}

	/**
	 * append to file last
	 * @param filename
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 */
	public native static long append(byte[] filename, byte[] data, int off, int len);
	
	/**
	 * write data to file offset
	 * @param filename
	 * @param fileoff
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 */
	public native static long write(byte[] filename, long fileoff, byte[] data, int off, int len);

	/**
	 * read data from file
	 * @param filename
	 * @param fileoff
	 * @param len
	 * @return
	 */
	public native static byte[] read(byte[] filename, long fileoff, int len);
	
	/**
	 * get file length
	 * @param filename
	 * @return
	 */
	public native static long filesize(byte[] filename);
	

	/* sql function, begin */
	
	/**
	 * save sql data to lexst db
	 * 
	 * on success, resolve data and call xxxCacheEntity
	 * on failed, other process
	 */
	public native static int insert(byte[] data);

	/**
	 * query sql
	 * @param metadata
	 * @return
	 */
	public native static int select(byte[] metadata);

	/**
	 * query and reply data
	 * @param stamp
	 * @return
	 */
	public native static byte[] nextSelect(int stamp, int size);
	
	/**
	 * delete data
	 * @param metadata
	 * @return
	 */
	public native static int delete(byte[] metadata);

	/**
	 * get delete's data
	 * @param stamp
	 * @param size
	 * @return
	 */
	public native static byte[] nextDelete(int stamp, int size);

	/**
	 * get reflex member identity
	 * @param stamp
	 * @return
	 */
	public native static byte[] findReflexLog(int stamp);
	
	/**
	 * get current chunk (cache model) data (prime site call)
	 * @param db
	 * @param table
	 * @param chunkid
	 * @return
	 */
	public native static byte[] getCacheReflex(byte[] db, byte[] table, long chunkid);

	/**
	 * save chunk data (cache model) to disk
	 * @param db
	 * @param table
	 * @param chunkid
	 * @param entity
	 * @return
	 */
	public native static int setCacheReflex(byte[] db, byte[] table, long chunkid, byte[] entity);
	
	/**
	 * delete chunk (cache model) from disk
	 * @param db
	 * @param table
	 * @param chunkid
	 * @return
	 */
	public native static int deleteCacheReflex(byte[] db, byte[] table, long chunkid);
	
	/**
	 * get current chunk data (final model) (prim site call)
	 * @param db
	 * @param table
	 * @param chunkid
	 * @return
	 */
	public native static byte[] getChunkReflex(byte[] db, byte[] table, long chunkid);
	
	/**
	 * update chunk data to disk (final model) (slave site)
	 * @param db
	 * @param table
	 * @param chunkid
	 * @param entity
	 * @return
	 */
	public native static int setChunkReflex(byte[] db, byte[] table, long chunkid, byte[] entity);


	/**
	 * force to chunk
	 * on success, >=0; otherwise <0
	 * @param db
	 * @param table
	 * @return
	 */
	public native static int rush(byte[] db, byte[] table);

	/* sql function, end */

	/**
	 * initialize database
	 * success return 0, otherwise other value
	 */
	public native static int initialize();

	/**
	 * 重构数据。<br>
	 * 将某个表按照某列重新排列数据，操作过程中，删除过期数据
	 * on success, result byte return 
	 * on failed, 0 byte return
	 *
	 * @param db
	 * @param table
	 * @return
	 */
	public native static byte[] rebuild(byte[] db, byte[] table, short columnId);

	/**
	 * set job threads
	 * @param num
	 * @return
	 */
	public native static int setWorker(int num);

	/**
	 * build directory
	 * @param path
	 * @return
	 */
	public native static int setBuildRoot(byte[] path);

	/**
	 * cache directory
	 * @param path
	 * @return
	 */
	public native static int setCacheRoot(byte[] path);

	/**
	 * chunk directory
	 * @param path
	 * @return
	 */
	public native static int setChunkRoot(byte[] path);

	/**
	 * 建立一个全新的表空间，如果有旧的表将删除
	 * @param metadata
	 * @return
	 */
	public native static int createSpace(byte[] metadata, boolean prime);

	/**
	 * 删除表空间下的所有数据块和目录
	 * @param db
	 * @param table
	 * @return
	 */
	public native static int deleteSpace(byte[] db, byte[] table);
	
	/**
	 * 初始化表空间，但是不启动
	 * @param metadata
	 * @return
	 */
	public native static int initSpace(byte[] metadata, boolean prime);

	/**
	 * 加载表记录，并且启动
	 * @param metadata
	 * @return
	 */
	public native static int loadSpace(byte[] metadata, boolean prime);

	/**
	 * stop and close space(cannot delete chunk)
	 * @param db
	 * @param table
	 * @return
	 */
	public native static int stopSpace(byte[] db, byte[] table);

	/**
	 * check a space exists
	 * @param db
	 * @param table
	 * @return
	 */
	public native static int findSpace(byte[] db, byte[] table);

	/**
	 * list all space name
	 * @return
	 */
	public native static byte[] listSpaces();

	/**
	 * load database and start thread
	 * @return
	 */
	public native static int launch();

	/**
	 * stop job thread and stop database
	 * @return
	 */
	public native static int stop();

	/**
	 * add a chunk id to jni server
	 * @param chunkId
	 */
	public native static int addChunkId(long chunkId);

	/**
	 * count free chunk identity number
	 * @return
	 */
	public native static int countFreeChunkIds();
	
	/**
	 * get all free chunk identity
	 * @return
	 */
	public native static long[] getFreeChunkIds();
	
	/**
	 * get all used chunk identity
	 * @return
	 */
	public native static long[] getTotalUsedChunkIds();
	
	/**
	 * return all chunk and chunk index
	 * @return
	 */
	public native static byte[] pullChunkIndex();

	/**
	 * flush index set by a space
	 * @param db
	 * @param table
	 * @return
	 */
	public native static byte[] findChunkIndex(byte[] db, byte[] table);

	/**
	 * check update
	 * @return
	 */
	public native static boolean isRefreshing();

	/**
	 * count disk space size
 	 * index 0: free size
 	 * index 1: used size
	 * @return
	 */
	public native static long[] getDiskSpace();

	/**
	 * set chunk size by a space
	 * @param db
	 * @param table
	 * @param size
	 * @return
	 */
	public native static int setChunkSize(byte[] db, byte[] table, int size);
	
	/**
	 * return a finish chunk 
	 * style : id(8 byte), db, table, chunk path
	 * @return
	 */
	public native static byte[] nextFinishChunk();

	/**
	 * return a chunk path
	 * @param db
	 * @param table
	 * @param chunkid
	 * @return
	 */
	public native static byte[] findChunkPath(byte[] db, byte[] table, long chunkid);

	/**
	 * return a cache chunk path
	 * @param db
	 * @param table
	 * @param chunkid
	 * @return
	 */
	public native static byte[] findCachePath(byte[] db, byte[] table, long chunkid);
	
	/**
	 * load a chunk file
	 * @param db
	 * @param table
	 * @param filename (disk file)
	 * @return
	 */
	public native static int loadChunk(byte[] db, byte[] table, byte[] filename);

	/**
	 * find a chunk file
	 * @param db
	 * @param table
	 * @param chunkId
	 * @return
	 */
	public native static int findChunk(byte[] db, byte[] table, long chunkId);

	/**
	 * @param db
	 * @param table
	 * @param chunkId
	 * @return
	 */
	public native static int deleteChunk(byte[] db, byte[] table, long chunkId);
	
	/**
	 * modify chunk to prime mode
	 * 
	 * @param db
	 * @param table
	 * @param chunkId
	 * @return
	 */
	public native static int toPrime(byte[] db, byte[] table, long chunkId);
	
	/**
	 * modify chunk to slave mode
	 * 
	 * @param db
	 * @param table
	 * @param chunkId
	 * @return
	 */
	public native static int toSlave(byte[] db, byte[] table, long chunkId);
	
	/**
	 * get all chunk identity(used) for space
	 * @param db
	 * @param table
	 * @return
	 */
	public native static long[] getChunkIds(byte[] db, byte[] table);
	
	/**
	 * get cache chunk identity(used) for space
	 * @param db
	 * @param table
	 * @return
	 */
	public native static long getCacheId(byte[] db, byte[] table);

	/**
	 * when chunk not exists, choose a chunk filename
	 * 
	 * @param db
	 * @param table
	 * @param chunkId
	 * @return
	 */
	public native static byte[] defineChunkPath(byte[] db, byte[] table, long chunkId);

	/**
	 * set site rank
	 * @param rank
	 */
	public native static void setRank(int rank);

	/**
	 * load index into memory
	 * @param db
	 * @param table
	 * @return
	 */
	public native static int inrush(byte[] db, byte[] table);

	/**
	 * clear index from memory
	 * @param db
	 * @param table
	 * @return
	 */
	public native static int uninrush(byte[] db, byte[] table);

	/**
	 * load chunk into memory
	 * @param db
	 * @param table
	 * @return
	 */
	public native static int afflux(byte[] db, byte[] table);

	/**
	 * clear chunk from memory
	 * @param db
	 * @param table
	 * @return
	 */
	public native static int unafflux(byte[] db, byte[] table);

	/**
	 * find build root path by a space 
	 * @param db
	 * @param table
	 * @return
	 */
	public native static byte[] getBuildPath(byte[] db, byte[] table);
	
	/**
	 * find cache root path by a space
	 * @param db
	 * @param table
	 * @return
	 */
	public native static byte[] getCachePath(byte[] db, byte[] table);
	
	/**
	 * find chunk root path by a space
	 * @param db
	 * @param table
	 * @param index
	 * @return
	 */
	public native static byte[] getChunkPath(byte[] db, byte[] table, int index);
	
	/**
	 * analyse all chunk
	 * @param db
	 * @param table
	 * 
	 * @return
	 * on success, marshal size (G, >0)
	 * space is empty, 0 return
	 * on failed, error code
	 */
//	public native static long[] marshal(byte[] db, byte[] table, short columnId);
	public native static int marshal(byte[] db, byte[] table, short columnId);
	
	/**
	 * export data
	 * @param db
	 * @param table
	 * @param readsize
	 * @return
	 */
	public native static byte[] educe(byte[] db, byte[] table, int readsize);
	
	/**
	 * pick analyse chunkid and chunkid count
	 * array format: chunkid(8 bytes) + chunk hit count(4 bytes)
	 * @return
	 */
	public native static byte[] getHitRecord();
	
	/**
	 * find the space name for chunk identity
	 * @param chunkid
	 * @return
	 */
	public native static byte[] findSpaceBy(long chunkid);
}