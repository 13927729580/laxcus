/**
 * 
 */
package com.lexst.data.pool;

import java.util.*;

import com.lexst.sql.schema.*;
import com.lexst.util.*;

final class ReflexLog {
	
	/** 版本号 **/
	private int version;

	/** 写入记录数 **/
	private int rows;

	/** 数据库表名称 **/
	private Space space;

	/** 缓存块标识号 **/
	private List<Long> caches = new ArrayList<Long>();

	/** 数据块标识号 **/
	private List<Long> chunks = new ArrayList<Long>();

	/**
	 * 
	 */
	public ReflexLog() {
		super();
	}
	
	public int getVersion() {
		return this.version;
	}

	public int getRows() {
		return this.rows;
	}

	public Space getSpace() {
		return this.space;
	}
	
	public List<Long> listCacheIdentity() {
		return this.caches;
	}
	
	public List<Long> listChunkIdentity() {
		return this.chunks;
	}

	/**
	 * resolve reflex identity information
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		if (seek + 10 > end) {
			throw new SizeOutOfBoundsException("size missing!");
		}

		this.version = Numeric.toInteger(b, seek, 4);
		seek += 4;
		this.rows = Numeric.toInteger(b, seek, 4);
		seek += 4;
		int schemaSize = b[seek] & 0xFF;
		seek += 1;
		int tableSize = b[seek] & 0xFF;
		seek += 1;

		if (seek + schemaSize + tableSize > end) {
			throw new SizeOutOfBoundsException("size missing!");
		}

		String schema = new String(b, seek, schemaSize);
		seek += schemaSize;
		String table = new String(b, seek, tableSize);
		seek += tableSize;

		this.space = new Space(schema, table);

		if (seek + 8 > end) {
			throw new SizeOutOfBoundsException("size missing!");
		}

		// 成员标识号总数
		int cacheElements = Numeric.toInteger(b, seek, 4);
		seek += 4;
		int chunkElements = Numeric.toInteger(b, seek, 4);
		seek += 4;

		// 检查标识号字节量
		if (seek + ((cacheElements + chunkElements) * 8) > end) {
			throw new SizeOutOfBoundsException("size missing!");
		}

		for (int i = 0; i < cacheElements; i++) {
			long cacheid = Numeric.toLong(b, seek, 8);
			seek += 8;
			caches.add(cacheid);
		}

		for (int i = 0; i < chunkElements; i++) {
			long chunkid = Numeric.toLong(b, seek, 8);
			seek += 8;
			chunks.add(chunkid);
		}

		return seek - off;
	}

}