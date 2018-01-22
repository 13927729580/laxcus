/**
 *
 */
package com.lexst.remote.client.data;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.zip.*;

import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.remote.client.*;
import com.lexst.sql.chunk.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.util.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.visit.naming.data.*;

public class DataClient extends ThreadClient implements DataVisit {

	private static Method methodNothing;
	private static Method methodApplyRank;
	private static Method methodFindChunk;
	private static Method methodRevive;
	private static Method methodDistribute;
	private static Method methodUpgrade;
	private static Method methodFindTable;
	private static Method methodCreateSpace;
	private static Method methodDeleteSpace;

	private static Method methodRebuild;
	private static Method methodLoadIndex;
	private static Method methodStopIndex;
	private static Method methodLoadChunk;
	private static Method methodStopChunk;
	
	private static Method methodShowChunkSize1;
	private static Method methodShowChunkSize2;
	
	private static Method methodSuckup;

	static {
		try {
			methodNothing = (DataVisit.class).getMethod("nothing", new Class<?>[0]);
			methodApplyRank = (DataVisit.class).getMethod("applyRank", new Class<?>[0]);
			methodFindChunk = (DataVisit.class).getMethod("findChunk", new Class<?>[] { String.class, String.class });
			methodDistribute = (DataVisit.class).getMethod("distribute", new Class<?>[] { SiteHost.class, String.class, String.class, Long.TYPE , Long.TYPE});
			methodUpgrade = (DataVisit.class).getMethod("upgrade", new Class<?>[] { SiteHost.class, String.class, String.class, long[].class, long[].class });
			methodRevive = (DataVisit.class).getMethod("revive", new Class<?>[] { String.class, String.class, SiteHost.class });
			methodFindTable = (DataVisit.class).getMethod("findIndex", new Class<?>[] { String.class, String.class });
			methodCreateSpace = (DataVisit.class).getMethod("createSpace", new Class<?>[] { Table.class });
			methodDeleteSpace = (DataVisit.class).getMethod("deleteSpace", new Class<?>[] { String.class, String.class });
			
			methodRebuild = (DataVisit.class).getMethod("rebuild", new Class<?>[] { String.class, String.class, Short.TYPE });
			methodLoadIndex = (DataVisit.class).getMethod("loadIndex", new Class<?>[] { String.class, String.class });
			methodStopIndex = (DataVisit.class).getMethod("stopIndex", new Class<?>[] { String.class, String.class });
			methodLoadChunk = (DataVisit.class).getMethod("loadChunk", new Class<?>[] { String.class, String.class });
			methodStopChunk = (DataVisit.class).getMethod("stopChunk", new Class<?>[] { String.class, String.class });
			
			methodShowChunkSize1 = (DataVisit.class).getMethod("showChunkSize", new Class<?>[] { String.class });
			methodShowChunkSize2 = (DataVisit.class).getMethod("showChunkSize", new Class<?>[] { String.class, String.class });
			
			methodSuckup = (DataVisit.class).getMethod("suckup", new Class<?>[] { Long.TYPE, Integer.TYPE, Long.TYPE, Long.TYPE });
		} catch (NoSuchMethodException exp) {
			throw new NoSuchMethodError("stub class initialization failed");
		}
	}
	
	// sql object array
	private LockArray<DataCommand> array = new LockArray<DataCommand>(10);

	/**
	 * 选择以流模式(TCP)或者包模式(UDP)连接服务器
	 * 
	 * @param stream - true，TCP连接；false，UDP连接
	 */
	public DataClient(boolean stream) {
		super(stream, DataVisit.class.getName());
		this.setRecvTimeout(180);
		this.setNumber(-1);
	}

	/**
	 * @param host
	 */
	public DataClient(boolean stream, SocketHost host) {
		this(stream);
		super.setRemote(host);
	}

	/**
	 * @param ip
	 * @param port
	 */
	public DataClient(boolean stream, InetAddress ip, int port) {
		this(stream, new SocketHost(SocketHost.TCP, ip, port));
	}

	/**
	 * @param remote
	 */
	public DataClient(SocketHost remote) {
		this(remote.getFamily() == SocketHost.TCP);
		super.setRemote(remote);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.Visit#nothing()
	 */
	@Override
	public void nothing() throws VisitException {
		super.invoke(DataClient.methodNothing, null);
		super.refreshTime();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#getRank()
	 */
	@Override
	public int applyRank() throws VisitException {
		super.refreshTime();
		Object param = super.invoke(DataClient.methodApplyRank, null);
		return ((Integer)param).intValue();
	}
	
	/**
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public ChunkStatus[] findChunk(Space space) throws VisitException {
		return findChunk(space.getSchema(), space.getTable());
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#findChunk(java.lang.String, java.lang.String)
	 */
	@Override
	public ChunkStatus[] findChunk(String schema, String table) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table };
		Object param = super.invoke(DataClient.methodFindChunk, params);
		return (ChunkStatus[]) param;
	}
	
	/**
	 * @param from
	 * @param space
	 * @param chunkid
	 * @return
	 * @throws VisitException
	 */
	public boolean distribute(SiteHost from, Space space, long chunkid, long length) throws VisitException {
		return distribute(from, space.getSchema(), space.getTable(), chunkid, length);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#distribute(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, long, long)
	 */
	@Override
	public boolean distribute(SiteHost from, String schema, String table,
			long chunkid, long length) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { from, schema, table, new Long(chunkid), new Long(length) };
		Object param = super.invoke(DataClient.methodDistribute, params);
		return ((Boolean) param).booleanValue();
	}
	
	/**
	 * @param from
	 * @param space
	 * @param oldIds
	 * @param newIds
	 * @return
	 * @throws VisitException
	 */
	public boolean upgrade(SiteHost from, Space space, long[] oldIds, long[] newIds) throws VisitException {
		return this.upgrade(from, space.getSchema(), space.getTable(), oldIds, newIds);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#upgrade(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, long[], long[])
	 */
	@Override
	public boolean upgrade(SiteHost from, String schema, String table, long[] oldIds, long[] newIds) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { from, schema, table, oldIds, newIds };
		Object param = super.invoke(DataClient.methodUpgrade, params);
		return ((Boolean) param).booleanValue();
	}
	
	/**
	 * @param space
	 * @param from
	 * @return
	 * @throws VisitException
	 */
	public boolean revive(Space space, SiteHost from) throws VisitException {
		return revive(space.getSchema(), space.getTable(), from);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#revive(java.lang.String, java.lang.String, com.lexst.util.host.SiteHost)
	 */
	@Override
	public boolean revive(String schema, String table, SiteHost from) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table, from };
		Object param = super.invoke(DataClient.methodRevive, params);
		return ((Boolean) param).booleanValue();
	}

	/**
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public IndexTable findIndex(Space space) throws VisitException {
		return findIndex(space.getSchema(), space.getTable());
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#findTable(java.lang.String, java.lang.String)
	 */
	@Override
	public IndexTable findIndex(String schema, String table) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table};
		Object param = super.invoke(DataClient.methodFindTable, params);
		return (IndexTable) param;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#createSpace(com.lexst.sql.schema.Table)
	 */
	@Override
	public boolean createSpace(Table table) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { table };
		Object param = super.invoke(DataClient.methodCreateSpace, params);
		return ((Boolean)param).booleanValue();
	}
	
	/**
	 * delete a table space
	 * 
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public boolean deleteSpace(Space space) throws VisitException {
		return deleteSpace(space.getSchema(), space.getTable());
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#removeSpace(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean deleteSpace(String schema, String table) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table };
		Object param = super.invoke(DataClient.methodDeleteSpace, params);
		return ((Boolean)param).booleanValue();
	}
	
	/**
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public boolean rebuild(Space space, short columnId) throws VisitException {
		return rebuild(space.getSchema(), space.getTable(), columnId);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#rebuild(java.lang.String, java.lang.String, short)
	 */
	@Override
	public boolean rebuild(String schema, String table, short columnId) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table, new Short(columnId) };
		Object param = super.invoke(DataClient.methodRebuild, params);
		return ((Boolean) param).booleanValue();
	}
	
	/**
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public boolean loadIndex(Space space) throws VisitException {
		return loadIndex(space.getSchema(), space.getTable());
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#loadIndex(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean loadIndex(String schema, String table) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table };
		Object param = super.invoke(DataClient.methodLoadIndex, params);
		return ((Boolean) param).booleanValue();
	}

	/**
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public boolean stopIndex(Space space) throws VisitException {
		return stopIndex(space.getSchema(), space.getTable());
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#stopIndex(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean stopIndex(String schema, String table) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table };
		Object param = super.invoke(DataClient.methodStopIndex, params);
		return ((Boolean) param).booleanValue();
	}

	/**
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public boolean loadChunk(Space space) throws VisitException {
		return loadChunk(space.getSchema(), space.getTable());
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#loadChunk(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean loadChunk(String schema, String table) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table };
		Object param = super.invoke(DataClient.methodLoadChunk, params);
		return ((Boolean) param).booleanValue();
	}

	/**
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public boolean stopChunk(Space space) throws VisitException {
		return stopChunk(space.getSchema(), space.getTable());
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#stopChunk(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean stopChunk(String schema, String table) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table };
		Object param = super.invoke(DataClient.methodStopChunk, params);
		return ((Boolean) param).booleanValue();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#showChunkSize(java.lang.String)
	 */
	@Override
	public long showChunkSize(String schema) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema };
		Object param = super.invoke(DataClient.methodShowChunkSize1, params);
		return ((Long) param).longValue();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#showChunkSize(java.lang.String, java.lang.String)
	 */
	@Override
	public long showChunkSize(String schema, String table) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { schema, table };
		Object param = super.invoke(DataClient.methodShowChunkSize2, params);
		return ((Long) param).longValue();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.data.DataVisit#suckup(long, int, long, long)
	 */
	@Override
	public byte[] suckup(long jobid, int mod, long begin, long end) throws VisitException {
		super.refreshTime();
		Object[] params = new Object[] { new Long(jobid), new Integer(mod), new Long(begin), new Long(end) };
		Object param = super.invoke(DataClient.methodSuckup, params);
		return (byte[])param;
	}
	
	/**
	 * 根据元信息，提供对应的数据流
	 * @param jobid
	 * @param field
	 * @return
	 * @throws VisitException
	 */
	public byte[] suckup(long jobid, com.lexst.sql.conduct.matrix.DiskField field) throws VisitException {
		return this.suckup(jobid, field.getMod(), field.getBegin(), field.getEnd());
	}

	/**
	 * download a chunk, from prime site
	 * @param space
	 * @param chunkId
	 * @return
	 * @throws IOException
	 */
	public Stream download(Space space, long chunkId, long breakpoint) throws IOException {
		Command cmd = new Command(Request.DATA, Request.DOWNLOAD_CHUNK);
		Stream request = new Stream(cmd);
		request.addMessage(new Message(Key.CHUNK_ID, chunkId));
		if (breakpoint > 0) request.addMessage(new Message(Key.CHUNK_BREAKPOINT, breakpoint));
		request.addMessage(new Message(Key.SCHEMA, space.getSchema()));
		request.addMessage(new Message(Key.TABLE, space.getTable()));
		return super.executeStream(request, false);
	}

	/**
	 * @param finder
	 * @param object
	 */
	public void select(DataTrustor finder, Select object) {
		array.add(new DataCommand(finder, object));
		if(this.isRunning()) this.wakeup();
	}

	/**
	 * @param finder
	 * @param object
	 */
	public void delete(DataTrustor finder, Delete object) {
		array.add(new DataCommand(finder, object));
		if(this.isRunning()) this.wakeup();
	}

	/**
	 * @param finder
	 * @param object
	 */
	public void conduct(DataTrustor finder, FromPhase object) {
		array.add(new DataCommand(finder, object));
		if(this.isRunning()) this.wakeup();
	}

	/**
	 * insert sql data to db
	 * @param data
	 * @param sync (synchronization or asynchronization)
	 * @return
	 */
	public int insert(byte[] data, boolean sync) {
		super.refreshTime();
		
		CRC32 checksum = new CRC32();
		checksum.update(data, 0, data.length);
		long sum = checksum.getValue();
		
		Logger.debug("DataClient.insert, data len:%d, crc32:%d", data.length, sum);

		Command cmd = new Command(Request.SQL, Request.SQL_INSERT);
		Stream request = new Stream(getRemote(), cmd);
		request.addMessage(Key.CHECKSUM_CRC32, sum);
		request.addMessage(Key.INSERT_MODE, (sync ? Value.INSERT_SYNC : Value.INSERT_ASYNC));

		request.setData(data);
		Stream resp = null;
		try {
			resp = super.executeStream(request, true);
		} catch (IOException exp) {
			super.close();
			Logger.error(exp);
		} catch (Throwable exp) {
			super.close();
			Logger.fatal(exp);
		}

		int items = -1;
		if (resp != null) {
			// resolve response data
			cmd = resp.getCommand();
			if (cmd.getResponse() == Response.ACCEPTED) {
				byte[] b = resp.getData();
				Logger.debug("DataClient.insert, result size %d", (b == null ? -1 : b.length));
				items = Numeric.toInteger(b);
			}
		}
		// unlock self (data client)
		super.unlock();
		return items;
	}

	/**
	 * execute "SQL select syntax"
	 */
	private void doSelect(DataCommand cmd) {
		this.refreshTime();
		
		DataTrustor finder = cmd.trustor;

		Command fixpcmd = new Command(Request.SQL, Request.SQL_SELECT);
		Stream request = new Stream(getRemote(), fixpcmd);
		Object[] params = new Object[] { cmd.method }; // select object
		
		// 执行SELECT
		Stream resp = null;
		try {
			request.setData(params);
			resp = super.executeStream(request, false);
		} catch (IOException exp) {
			super.close();
			Logger.error(exp);
		} catch (Throwable exp) {
			super.close();
			Logger.fatal(exp);
		}

		if(resp == null) {
			finder.flushEmpty(this);
			return;
		}
		// check command
		fixpcmd = resp.getCommand();
		if (fixpcmd.getResponse() != Response.SELECT_FOUND) {
			Logger.warning("DataClient.doSelect, cannot find from %s - %s", getRemote(), ((Select)cmd.method).getSpace());
			finder.flushEmpty(this);
			return;
		}

		// item size
		long items = 0;
		Message msg = resp.findMessage( Key.CONTENT_ITEMS );
		if(msg != null) {
			items = msg.longValue();
		}
		// data size
		int datalen = resp.getContentLength();
		
		Logger.debug("DataClient.doSelect, Content Items:%d, Content Length:%d", items, datalen);

		byte[] bytes = null;
		try {
			bytes = resp.readContent();
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.error(exp);
		}

		// flush to memory
		finder.flushTo(this, items, bytes, 0, (bytes == null ? 0 : bytes.length));
	}
	
	/**
	 * 执行Conduct操作
	 * 
	 * @param cmd
	 */
	private void doConduct(DataCommand cmd) {
		this.refreshTime();
		
		DataTrustor finder = cmd.trustor;

		Command fixpcmd = new Command(Request.SQL, Request.SQL_CONDUCT);
		Stream request = new Stream(getRemote(), fixpcmd);
		Object[] objects = new Object[] { cmd.phase }; 
		
		// 执行CONDUCT
		Stream resp = null;
		try {
			request.setData(objects);
			resp = super.executeStream(request, false);
		} catch (IOException exp) {
			super.close();
			Logger.error(exp);
		} catch (Throwable exp) {
			super.close();
			Logger.fatal(exp);
		}
		if(resp == null) {
			finder.flushEmpty(this);
			return;
		}
		// check command
		fixpcmd = resp.getCommand();
		if (fixpcmd.getResponse() != Response.CONDUCT_OKAY) {
			Logger.warning("DataClient.doConduct, cannot find from %s", getRemote());
			finder.flushEmpty(this);
			return;
		}

		// item size
		long items = 0;
		Message msg = resp.findMessage( Key.CONTENT_ITEMS );
		if(msg != null) {
			items = msg.longValue();
		}
		// data size
		int datalen = resp.getContentLength();
		
		Logger.debug("DataClient.doConduct, Content Items:%d, Content Length:%d", items, datalen);

		byte[] bytes = null;
		try {
			bytes = resp.readContent();
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.error(exp);
		}

		// 数据输出到缓存
		finder.flushTo(this, items, bytes, 0, (bytes == null ? 0 : bytes.length));
	}
	
	/**
	 * execute "SQL delete syntax"
	 */
	private void doDelete(DataCommand cmd) {
		this.refreshTime();
		
		DataTrustor finder = cmd.trustor;

		Command fixpcmd = new Command(Request.SQL, Request.SQL_DELETE);
		Stream request = new Stream(getRemote(), fixpcmd);
		Object[] params = new Object[] { cmd.method }; // delete object
		// 执行SQL DELETE
		Stream resp = null;
		try {
			request.setData(params);
			resp = super.executeStream(request, false);
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (resp == null) {
			finder.flushEmpty(this);
		} else {
			fixpcmd = resp.getCommand();
			
			byte[] data = null;
			try {
				data = resp.readContent();
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}

			if (fixpcmd.getResponse() == Response.DELETE_FOUND) {
				Integer value = resp.findInt(Key.CONTENT_ITEMS);
				long items = (value == null ? 0 : value.intValue());
				finder.flushTo(this, items, data, 0, (data == null ? 0 : data.length));
			} else {
				finder.flushEmpty(this);
			}
			
		}
	}

	/**
	 * execute "select" and "delete"
	 */
	private void subprocess() {
		while(!array.isEmpty()) {
			DataCommand cmd = array.poll();
			if(cmd == null) {
				Logger.fatal("DataClient.subprocess, null DataCommand object, size:%d", array.size());
				continue;
			}
			
			switch(cmd.method.getMethod()) {
			case Compute.SELECT_METHOD:
				doSelect(cmd);
				break;
			case Compute.DELETE_METHOD:
				doDelete(cmd);
				break;
			case Compute.CONDUCT_METHOD:
				doConduct(cmd);
				break;
			}
		}
		super.unlock();
	}

	/**
	 * active connect
	 */
	private boolean active() {
		if (!super.lock()) return false;
		try {
			this.nothing();
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			this.unlock();
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.remote.client.ThreadClient#execute()
	 */
	@Override
	protected void execute() {
		while (!isInterrupted()) {
			if (array.size() > 0) {
				subprocess();
			} else {
				if (isRefreshTimeout(20000)) {
					if (!active()) { delay(500); continue; }
				}
				this.delay(5000);
			}
		}
	}

}