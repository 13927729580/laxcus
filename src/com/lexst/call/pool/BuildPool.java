/**
 * 
 */
package com.lexst.call.pool;

import java.util.*;

import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.remote.client.home.*;
import com.lexst.sql.chunk.*;
import com.lexst.util.naming.*;
import com.lexst.visit.*;

/**
 * 
 * 
 *
 */
public class BuildPool extends JobPool {

	private static BuildPool selfHandle = new BuildPool();

	private Map<Naming, ChunkSet> mapNaming = new TreeMap<Naming, ChunkSet>();

	/**
	 * 
	 */
	private BuildPool() {
		super();
	}

	public static BuildPool getInstance() {
		return BuildPool.selfHandle;
	}
	
	/**
	 * @param name
	 * @return
	 */
	public long[] findBuildChunk(String name) {
		Naming naming = new Naming(name);

		super.lockMulti();
		try {
			ChunkSet set = mapNaming.get(naming);
			if (set != null) {
				return set.toArray();
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}

		HomeClient client = super.bring();
		if(client == null) return null;
		
		long[] chunkIds = null;
		try {
			chunkIds = client.findBuildChunk(name);
		} catch (VisitException exp) {
			Logger.error(exp);
		}
		super.complete(client);

		super.lockSingle();
		try {
			if (chunkIds != null && chunkIds.length > 0) {
				ChunkSet set = new ChunkSet(chunkIds);
				mapNaming.put(naming, set);
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
		
		return chunkIds;
	}

	private void check() {
		ArrayList<Naming> a = new ArrayList<Naming>();
		super.lockMulti();
		try {
			a.addAll(mapNaming.keySet());
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		if (a.isEmpty()) return;

		HomeClient client = super.bring();
		if(client == null) return;		

		for (Naming naming : a) {
			super.lockSingle();
			try {
				long[] chunkIds = client.findBuildChunk(naming.get());
				ChunkSet set = mapNaming.get(naming);
				set.clear();
				if (chunkIds != null) set.add(chunkIds);
			} catch (VisitException exp) {
				Logger.error(exp);
			} finally {
				super.unlockSingle();
			}
		}

		super.complete(client);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("BuildPool.process, into ...");
		while (!super.isInterrupted()) {
			this.delay(20000);
			this.check();
		}
		Logger.info("BuildPool.process, exit");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		mapNaming.clear();
	}

}
