/**
 * 
 */
package com.lexst.home.pool;

import java.util.*;

import com.lexst.sql.chunk.*;

final class ChunkInfoSet {
	
	private Map<Long, ChunkStatus> map = new TreeMap<Long, ChunkStatus>();
	
	public ChunkInfoSet() {
		super();
	}
	
	public boolean add(ChunkStatus info) {
		return map.put(info.getId(), info) == null;
	}

	public ChunkStatus find(long chunkid) {
		return map.get(chunkid);
	}
}
