/**
 * 
 */
package com.lexst.site.build;

import java.io.*;
import java.util.*;

import com.lexst.sql.chunk.*;
import com.lexst.sql.schema.*;
import com.lexst.util.naming.*;

/**
 * 一个命名下允许有个数据库有记录
 *
 */
public class BuildSpace implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	/** task naming */
	private Naming naming;
	
	private Map<Space, ChunkSet> mapSpace = new TreeMap<Space, ChunkSet>();

	/**
	 * 
	 */
	public BuildSpace() {
		super();
	}

	/**
	 * @param naming
	 */
	public BuildSpace(Naming naming) {
		this();
		this.setNaming(naming);
	}
	
	public void setNaming(Naming s) {
		this.naming = new Naming(s);
	}

	public Naming getNaming() {
		return this.naming;
	}	

	public boolean isEmpty() {
		return mapSpace.isEmpty();
	}

	public int size() {
		return mapSpace.size();
	}
	
	public Set<Space> keySet() {
		return mapSpace.keySet();
	}

	public boolean exists(Space space) {
		return mapSpace.get(space) != null;
	}
	
	public boolean exists(Space space, long chunkId) {
		ChunkSet array = mapSpace.get(space);
		if (array != null) {
			return array.exists(chunkId);
		}
		return false;
	}
	
	public ChunkSet find(Space space) {
		return mapSpace.get(space);
	}

	public boolean add(Space space) {
		ChunkSet set = mapSpace.get(space);
		if (set != null) {
			return false;
		}
		set = new ChunkSet();
		return mapSpace.put(space, set) == null;
	}
	
	public boolean add(Space space, long chunkId) {
		ChunkSet set = mapSpace.get(space);
		if(set == null) {
			set = new ChunkSet();
			mapSpace.put(space, set);
		}
		return set.add(chunkId);
	}
	
	public int add(Space space, long[] chunkIds) {
		ChunkSet set = mapSpace.get(space);
		if (set == null) {
			set = new ChunkSet();
			mapSpace.put(space, set);
		}
		return set.add(chunkIds);
	}

	public boolean remove(Space space) {
		ChunkSet set = mapSpace.remove(space);
		return set != null;
	}
	
	public boolean remove(Space space, long chunkid) {
		ChunkSet set = mapSpace.get(space);
		if(set == null) return false;
		boolean success = set.remove(chunkid);
		if (set.isEmpty()) mapSpace.remove(space);
		return success;
	}
	
	public int remove(Space space, long[] chunkIds) {
		int count = 0;
		ChunkSet set = mapSpace.get(space);
		if (set != null) {
			count = set.remove(chunkIds);
			if (set.isEmpty()) mapSpace.remove(space);
		}
		return count;
	}

}