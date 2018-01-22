/**
 * 
 */
package com.lexst.data.pool;

import java.io.*;
import com.lexst.data.*;
import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.util.*;

/**
 * 
 * 提供SQL服务接入服务的数据管理池。<br>
 *
 */
public class SQLPool extends JobPool {
	
	// static instance
	private static SQLPool selfHandle = new SQLPool();
		
//	private Map<SiteHost, ClientSet> mapClient = new TreeMap<SiteHost, ClientSet>();
	
	/**
	 * default constructor
	 */
	private SQLPool() {
		super();
	}

	/**
	 * @return
	 */
	public static SQLPool getInstance() {
		return SQLPool.selfHandle;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("SQLPool.process, into...");
		while(!isInterrupted()) {
//			this.check();
			this.delay(20000);
		}
		Logger.info("SQLPool.process, exit");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
//		stopClients();
	}

	/**
//	 * stop all client
//	 */
//	private void stopClients() {
//		if(mapClient.isEmpty()) return;
//		
//		super.lockSingle();
//		try {
//			ArrayList<SiteHost> array = new ArrayList<SiteHost>(mapClient.keySet());
//			for (SiteHost host : array) {
//				ClientSet set = mapClient.remove(host);
//				int size = set.size();
//				for (int i = 0; i < size; i++) {
//					WorkClient client = (WorkClient) set.get(i);
//					client.stop();
//				}
//			}
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//	}
	
//	/**
//	 * find a work client
//	 * @param host
//	 * @param stream
//	 * @return
//	 */
//	private WorkClient findClient(SiteHost host, boolean stream) {
//		ClientSet set = null;
//		super.lockMulti();
//		try {
//			set = mapClient.get(host);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//		
//		if (set != null && set.size() > 0) {
//			WorkClient client = (WorkClient) set.lockNext();
//			if (client != null) return client;
//			if (set.size() >= ClientSet.LIMIT) {
//				client = (WorkClient) set.next();
//				client.locking();
//				return client;
//			}
//		}
//		
//		boolean success = false;
//		WorkClient client = new WorkClient(stream);
//		// connect to host
//		SocketHost address = (stream ? host.getTCPHost() : host.getUDPHost());
//		try {
//			client.connect(address);
//			success = true;
//		} catch (IOException exp) {
//			Logger.error(exp);
//		}
//		if (!success) {
//			client.close();
//			return null;
//		}
//		
////		client.locking();	// locked client
//		client.start(); 	// start client thread
//
//		super.lockSingle();
//		try {
//			if(set == null) {
//				set = new ClientSet();
//				mapClient.put(host, set);
//			}
//			set.add(client);
//		} catch(Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//
//		return client;
//	}
	
//	/**
//	 * connect host
//	 * @param host
//	 * @return
//	 * @throws IOException
//	 * @throws VisitException
//	 */
//	private WorkClient findClient(SiteHost host) {
//		return findClient(host, true);
//	}

	private Stream buildReply(short code) {
		Command cmd = new Command(code);
		return new Stream(cmd);
	}

	private void flushSelectResult(OutputStream resp, short code, int items, byte[] data) throws IOException {
		Stream reply = buildReply(code);
		reply.setContentItems(items);
		byte[] bytes = null;
		if (data != null && data.length > 0) {
			bytes = reply.buildHead(data.length);
		} else {
			bytes = reply.build();
		}
		resp.write(bytes, 0, bytes.length);
		if (data != null && data.length > 0) {
			resp.write(data, 0, data.length);
		}
		resp.flush();
	}

//	/**
//	 * 输出分布计算结果的信息流
//	 * @param resp
//	 * @param code
//	 * @param data
//	 * @throws IOException
//	 */
//	private void flushDistributeResult(OutputStream resp, short code, byte[] data) throws IOException {
//		Stream reply = buildReply(code);
//		byte[] bytes = null;
//		if (data != null && data.length > 0) {
//			bytes = reply.buildHead(data.length);
//		} else {
//			bytes = reply.build();
//		}
//		resp.write(bytes, 0, bytes.length);
//		if (data != null && data.length > 0) {
//			resp.write(data, 0, data.length);
//		}
//		resp.flush();
//	}

//	private void direct_flush(ByteArrayOutputStream buff, Direct direct, OutputStream resp) throws IOException {
//		Naming naming = direct.getFrom().getTaskNaming();
//		DirectDiffuseTask task = (DirectDiffuseTask)FromTaskPool.getInstance().find(naming);
//		if (task == null) {
//			Logger.error("SQLPool.direct_flush, cannot find direct-task: '%s'", naming);
//			flushDistributeResult(resp, Response.DISTRIBUTE_FAILED, null);
//			return;
//		}
//		
//		byte[] data = null;
//		if (buff != null) data = buff.toByteArray();
//		// split data, from to_sites
//		DirectResult[] results = task.execute(direct, data); 
//		// release memory
//		data = null;
//		if (buff != null) buff.reset();
//
//		List<SiteHost> to_sites = direct.getTo().listSites();
//		if (results == null || results.length == 0) { // this is error
//			flushDistributeResult(resp, Response.DISTRIBUTE_FAILED, null);
//			return;
//		} else if (results.length != to_sites.size()) {
//			flushDistributeResult(resp, Response.DISTRIBUTE_SIZENOTMATCH, null);
//			return;
//		}
//
//		WorkTrustor finder = new WorkTrustor(results.length);
//		for (int i = 0; i < results.length; i++) {
//			SiteHost host = to_sites.get(i);
//			WorkClient client = findClient(host);
//			if(client == null) { // this is error
//				Logger.error("SQLPool.direct_flush, cannot connect %s", host);
//				break;
//			}
//			
//			Direct clone_direct = (Direct) direct.clone();
//			clone_direct.getTo().setSites(results.length);
//			// 保存WORK连接句柄和数据，准备进入"AGGREGATE"状态 
//			finder.add(client, clone_direct, results[i].data());
//		}
//
//		if (finder.size() != results.length) {
//			Logger.error("SQLPool.direct_flush, not match!");
//			finder.disconnect(true);
//			flushDistributeResult(resp, Response.DISTRIBUTE_CLIENTERR, null);
//			return;
//		}
//		Logger.debug("SQLPool.direct_flush, client size:%d", finder.size());
//
//		// start jobs
//		finder.launch();
//		// wait jobs...
//		finder.waiting();
//		// get data
//		data = finder.data();
//
//		Logger.debug("SQLPool.direct_flush, from '%s' to '%s', data size %d",
//				direct.getFrom().getTaskNaming(), direct.getTo().getTaskNaming(), (data == null ? -1 : data.length));
//
//		if (data == null || data.length == 0) {
//			flushDistributeResult(resp, Response.DISTRIBUTE_FAILED, null);
//		} else {
//			flushDistributeResult(resp, Response.DISTRIBUTE_OKAY, data);
//		}
//	}

//	/**
//	 * @param buff
//	 * @param conduct
//	 * @param resp
//	 * @throws IOException
//	 */
//	private void conduct_flush(ByteArrayOutputStream buff, Conduct conduct, OutputStream resp) throws IOException {
//		// check table
//
//		// 进入from的SELECT只能有一个
//		FromObject from = conduct.getFrom();
//		if(from.countSelect() == 1) {
//			Select select = from.getSelects().get(0);
//			Space space = select.getSpace();
//			Naming naming = conduct.getFrom().getTaskNaming();
//			Project project = FromTaskPool.getInstance().findProject(naming);
//			Table table = project.getTable(space);
//			if (table == null) {
//				table = com.lexst.data.Launcher.getInstance().findTable(space);
//				if (table == null) {
//					Logger.error("SQLPool.conduct_flush, cannot find table: '%s'", space);
//					flushDistributeResult(resp, Response.DISTRIBUTE_FAILED, null);
//					return;
//				}
//				project.setTable(space, table);
//			}
//		}
//
//		// 根据FROM命名，找到匹配的对象实例
//		Naming naming = conduct.getFrom().getTaskNaming();
//		ConductFromTask task = (ConductFromTask)FromTaskPool.getInstance().find(naming);
//		if (task == null) {
//			Logger.error("SQLPool.conduct_flush, cannot find conduct-task '%s'", naming);
//			flushDistributeResult(resp, Response.DISTRIBUTE_FAILED, null);
//			return;
//		}
//
//		byte[] data = null;
//		if (buff != null) data = buff.toByteArray();
//		SiteHost local = Launcher.getInstance().getLocal().getHost();
//		Area results = task.divideup(local, DiskPool.getInstance(), conduct, data, 0, data.length);
//		data = null;
//		if (buff != null) buff.reset();
//
//		// build Area to bytes
//		ByteArrayOutputStream mem = new ByteArrayOutputStream();
////		for (int i = 0; results != null && i < results.length; i++) {
////			byte[] b = results[i].build();
////			mem.write(b, 0, b.length);
////		}
//		
//		byte[] b = results.build();
//		mem.write(b, 0, b.length);
//		
//		data = mem.toByteArray();
//		
//		Logger.debug("SQLPool.conduct_flush,  conduct byte size:%d",  data.length);
//		
//		this.flushDistributeResult(resp, Response.DISTRIBUTE_OKAY, data);
//	}
	
//	/**
//	 * query data from database
//	 * @param resp
//	 * @param query
//	 * @throws IOException
//	 */
//	public void select(Select select, OutputStream resp) throws IOException {
//		long time = System.currentTimeMillis();
//		byte[] query = select.build();
//		
//		// jni query
//		long result = Install.select(query);
//		int rows = (int) ((result >>> 32) & 0xffffffffL); //item count
//		int stamp = (int) (result & 0xffffffffL);	// query stamp
//
//		Logger.debug("SQLPool.select, data rows:%d, stamp:%d", rows, stamp);
//
//		// read sql data
//		boolean finish = (rows == 0);
//		int readsize = 1024 * 1024;
//		ByteArrayOutputStream buff = new ByteArrayOutputStream(finish ? 16 : readsize);
//		while (!finish) {
//			byte[] b = Install.nextSelect(stamp, readsize);
//			// send to
//			if (b == null || b.length == 0) {
//				finish = true;
//			} else {
//				finish = (b[0] == 1);
//				buff.write(b, 1, b.length - 1);
//			}
//		}
//		
//		Logger.debug("SQLPool.select, data len:%d, query usedtime:%d", buff.size(), System.currentTimeMillis()-time);
//
//		byte[] sqldata = buff.toByteArray();
//		if (sqldata == null || sqldata.length == 0) {
//			this.flushSelectResult(resp, Response.SELECT_NOTFOUND, rows, null);
//		} else {
//			this.flushSelectResult(resp, Response.SELECT_FOUND, rows, sqldata);
//		}
//	}

	/**
	 * 执行SQL SELECT检索
	 * 
	 * @param resp
	 * @param query
	 * @throws IOException
	 */
	public void select(Select select, OutputStream resp) throws IOException {
//		long time = System.currentTimeMillis();
		// 元命令
		byte[] meta = select.build();
		// 启动JNI数据检索，返回检索标记
		int stamp = Install.select(meta);
		
		Logger.debug("SQLPool.select, stamp:%d", stamp);

		if(stamp < 0) {
			this.flushSelectResult(resp, Response.SERVER_ERROR, 0, null);
			return;
		} else if(stamp == 0) {
			this.flushSelectResult(resp, Response.SELECT_NOTFOUND, 0, null);
			return;
		}
		
		// 读第一段数据流(每次1M的数据量)
		byte[] data = Install.nextSelect(stamp, 0x100000); //1M

		// 分析报头，确定全部数据流长度(标记头和检索数据)
		AnswerFlag flag = new AnswerFlag();
		int size = flag.resolve(data, 0, data.length);
		int rows = flag.getRows();
		long allsize = size + flag.getSize();

		ByteArrayOutputStream buff = new ByteArrayOutputStream( (int)allsize );
		buff.write(data, 0, data.length);
		
		// 从JNI接口，继续读取下一段数据
		long seek = data.length;
		while(seek < allsize) {
			data = Install.nextSelect(stamp, 0x100000);
			buff.write(data, 0, data.length);
			seek += data.length;
		}
		
		data = buff.toByteArray();
		this.flushSelectResult(resp, Response.SELECT_FOUND, rows, data);
	}

//	/**
//	 * delete data from database
//	 * @param resp
//	 * @param syntax
//	 */
//	public void delete(Delete object, OutputStream resp) throws IOException {
//		// delete item
//		byte[] syntax = object.build();
//		byte[] ret = Install.delete(syntax);
//
//		int off = 0;
//		int items = Numeric.toInteger(ret, off, 4);
//		off += 4;
//		int stamp = Numeric.toInteger(ret, off, 4);
//		off += 4;
//		
//		Logger.debug("SQLPool.delete, delete count:%d, stamp:%d", items, stamp);
//		
//		if (items < 0) { // error
//			Stream stream = buildReply(Response.SERVER_ERROR);
//			byte[] b = stream.build();
//			resp.write(b, 0, b.length);
//			resp.flush();
//			return;
//		} else if (items == 0) { // not found
//			Stream stream = buildReply(Response.DELETE_NOTFOUND);
//			// this is not found, send to socket
//			byte[] b = stream.build();
//			resp.write(b, 0, b.length);
//			resp.flush();
//			return;
//		} else { // delete success
//			// snatch
//			byte[] sqldata = null;
//			if (object.isSnatch()) {
//				int readsize = 0x100000; // 1024 * 1024;
//				ByteArrayOutputStream buff = new ByteArrayOutputStream(readsize);
//				boolean finish = false;
//				while (!finish) {
//					byte[] bytes = Install.nextDelete(stamp, readsize);
//					// when finish, exit
//					if (bytes == null || bytes.length == 0) {
//						finish = true;
//					} else {
//						// send to socket
//						finish = (bytes[0] == 1);
//						buff.write(bytes, 1, bytes.length - 1);
//					}
//				}
//				sqldata = buff.toByteArray();
//			}
//
//			Stream stream = this.buildReply(Response.DELETE_FOUND);
//			stream.addMessage(new Message(Key.CONTENT_ITEMS, items));
//			byte[] head = stream.buildHead(sqldata == null ? 0 : sqldata.length);
//			resp.write(head, 0, head.length);
//			if (sqldata != null && sqldata.length > 0) {
//				resp.write(sqldata, 0, sqldata.length);
//			}
//			resp.flush();
//		}
//		
//		// backup chunk
//		if(items > 0) {
//			int dbsz = ret[off++] & 0xFF;
//			int tabsz = ret[off++] & 0xFF;
//			
//			byte[] db = new byte[dbsz];
//			byte[] table = new byte[tabsz];
//			
//			System.arraycopy(ret, off, db, 0, db.length);
//			off += db.length;
//			System.arraycopy(ret, off, table, 0, table.length);
//			off += table.length;
//			
//			long cacheid = Numeric.toLong(ret, off, 8);
//			off += 8;
//			if(cacheid != 0L) {
//				byte[] entity = Install.getCacheEntity(db, table, cacheid);
////				byte[] entity = Install.getCacheUpdate(db, table, cacheid);
//				CachePool.getInstance().update(new String(db), new String(table), cacheid, entity);
//			}
//			
//			while (off < ret.length) {
//				long chunkid = Numeric.toLong(ret, off, 8);
//				off += 8;
//				byte[] entity = Install.getChunkEntity(db, table, chunkid);
////				byte[] entity = Install.getChunkUpdate(db, table, chunkid);
//				ChunkPool.getInstance().update(new String(db), new String(table), chunkid, entity);
//			}
//		}
//	}
	
	/**
	 * delete data from database
	 * @param resp
	 * @param syntax
	 */
	public void delete(Delete object, OutputStream resp) throws IOException {
		// delete item
		byte[] syntax = object.build();
		int stamp = Install.delete(syntax);
		
		Logger.debug("SQLPool.delete, stamp:%d", stamp);
		
		if(stamp < 0) { // error code
			Stream stream = buildReply(Response.SERVER_ERROR);
			byte[] b = stream.build();
			resp.write(b, 0, b.length);
			resp.flush();
			return;
		} else if(stamp == 0) { // not found
			Stream stream = buildReply(Response.DELETE_NOTFOUND);
			// this is not found, send to socket
			byte[] b = stream.build();
			resp.write(b, 0, b.length);
			resp.flush();
			return;
		} 
				
		ByteArrayOutputStream buff = new ByteArrayOutputStream( 0x100000 );
		int rows = 0;
		if(!object.isSnatch()) {
			rows = stamp;
		} else {
			// 读数据流(每次1M）
			byte[] data = Install.nextDelete(stamp, 0x100000);
			
			// 解析标记信息
			AnswerFlag flag = new AnswerFlag();
			int size = flag.resolve(data, 0, data.length);
			
			// 确定行记录总数和总的字节长度
			rows = flag.getRows();
			long allsize = size + flag.getSize();
			buff.write(data, 0, data.length);
			
			// 继续读第二段及以后数据
			long seek = data.length;
			while(seek < allsize) {
				data = Install.nextDelete(stamp, 0x100000);
				buff.write(data, 0, data.length);
				seek += data.length;
			}
			data = buff.toByteArray();
		}
		
		Stream stream = this.buildReply(Response.DELETE_FOUND);
		stream.addMessage(new Message(Key.CONTENT_ITEMS, rows));
		int size = buff.size();
		byte[] head = stream.buildHead(size);
		resp.write(head, 0, head.length);
		if (size > 0) {
			byte[] data = buff.toByteArray();
			resp.write(data, 0, data.length);
		}
		resp.flush();

		// transmit data
		byte[] log = Install.findReflexLog(stamp);
		
		ReflexLog tag = new ReflexLog();
		tag.resolve(log, 0, log.length);

		Space space = tag.getSpace();
		byte[] schema = space.getSchema().getBytes();
		byte[] table = space.getTable().getBytes();
		for(long cacheid : tag.listCacheIdentity()) {
			byte[] entity = Install.getCacheReflex(schema, table, cacheid);
			CachePool.getInstance().update(space, cacheid, entity);			
		}

		for(long chunkid : tag.listChunkIdentity()) {
			byte[] entity = Install.getChunkReflex(schema, table, chunkid);
			ChunkPool.getInstance().update(space, chunkid, entity);
		}

	}
	
	/**
	 * flush "INSERT" result
	 * @param items
	 * @param resp
	 * @throws IOException
	 */
	private void flushInsert(int items, OutputStream resp) throws IOException {
		boolean success = items > 0;
		Logger.note(success, "SQLPool.flushInsert, item count:%d", items);

		Command cmd = new Command(success ? Response.ACCEPTED : Response.DATA_INSERT_FAILED);
		Stream reply = new Stream(cmd);
		reply.setData(Numeric.toBytes(items));

		// flush to network
		byte[] data = reply.build();
		resp.write(data, 0, data.length);
		resp.flush();
	}
	
//	/**
//	 * sync append data to lexst db
//	 * @param data
//	 * @param resp
//	 * @return
//	 * @throws IOException
//	 */
//	private boolean syncInsert(byte[] data, OutputStream resp) throws IOException {
//		Logger.debug("SQLPool.syncInsert, data len:%d", data.length);
//		int off = 0;
//		// all size
//		int allsize = Numeric.toInteger(data, off, 4);
//		off += 4;
//		if (data.length != allsize) {
//			flushInsert(-1, resp);
//			return false;
//		}
//		// version
//		int version = Numeric.toInteger(data, off, 4);
//		off += 4;
//		if (version != 1) {
//			flushInsert(-1, resp);
//			return false;
//		}
//		// space
//		int dbsize = data[off++];
//		int tbsize = data[off++];
//		
//		String db = new String(data, off, dbsize);
//		off += dbsize;
//		String table = new String(data, off, tbsize);
//		off += tbsize;
//		Space space = new Space(db, table);
//		// check space
//		if (!Launcher.getInstance().existSpace(space)) {
//			Logger.error("SQLPool.syncInsert, not found space '%s'", space);
//			flushInsert(-1, resp);
//			return false;
//		}
//
//		Logger.debug("SQLPool.syncInsert, flush to '%s'", space);
//		long time = System.currentTimeMillis();
//		
//		int items = 0;
//		while(true) {
//			// insert data (JNI)
//			byte[] ret = Install.insert(data);
//			// check result
//			off = 0;
//			byte status = ret[off++];
//			items = Numeric.toInteger(ret, off, 4);
//			off += 4;
//			if(status == 0) { // success
//				int dblen = ret[off++] & 0xFF;
//				int tablen = ret[off++] & 0xFF;
//				byte[] db_name = new byte[dblen];
//				byte[] table_name = new byte[tablen];
//				// db name
//				System.arraycopy(ret, off, db_name, 0, db_name.length);
//				off += dblen;
//				// table name
//				System.arraycopy(ret, off, table_name, 0, table_name.length);
//				off += tablen;
//				// chunk identity
//				long chunkid = Numeric.toLong(ret, off, 8);
//				off += 8;
//				// get cache data
//				byte[] entity = Install.getCacheEntity(db_name, table_name, chunkid);
//				// send to CachePool, distribute to other host
//				CachePool.getInstance().update(new String(db_name), new String(table_name), chunkid, entity);
//				break;
//			} else {
//				if (items == -3) { // get new chunkid
//					while (true) {
//						long[] chunkIds = Launcher.getInstance().applyChunkId(5);
//						if (chunkIds != null) {
//							for (long chunkId : chunkIds) {
//								Install.addChunkId(chunkId);
//							}
//							break;
//						}
//					}
//				} else if (items == -2) { // memory out
//					break;
//				} else { // other error
//					break;
//				}
//			}
//		}
//
//		Logger.debug("SQLPool.syncInsert, flush '%s' to disk, insert count:%d, insert usedtime:%d",
//				space, items, System.currentTimeMillis()-time);
//
//		// response data
//		flushInsert(items, resp);
//		return items > 0;
//	}
	
	/**
	 * 同步写入JNI数据库
	 * @param data
	 * @param resp
	 * @return
	 * @throws IOException
	 */
	private boolean syncInsert(byte[] data, OutputStream resp) throws IOException {
		Logger.debug("SQLPool.syncInsert, data len:%d", data.length);
		
//		int seek = 0;
//		// all size
//		int allsize = Numeric.toInteger(data, seek, 4);
//		seek += 4;
//		if (data.length != allsize) {
//			flushInsert(-1, resp);
//			return false;
//		}
//		// version
//		int version = Numeric.toInteger(data, seek, 4);
//		seek += 4;
//		if (version != 1) {
//			flushInsert(-1, resp);
//			return false;
//		}
//		// space
//		int schemaSize = data[seek++] & 0xFF;
//		int tableSize = data[seek++] & 0xFF;
//		
//		String schema = new String(data, seek, schemaSize);
//		seek += schemaSize;
//		String table = new String(data, seek, tableSize);
//		seek += tableSize;
//		Space space = new Space(schema, table);
		
		// 解析插入数据标识头
		InsertFlag flag = null;
		DefaultInsert def = new DefaultInsert();
		try {
			flag = def.resolveFlag(data, 0, data.length);
		} catch (RuntimeException e) {
			Logger.error(e);
		} catch (Throwable e) {
			Logger.fatal(e);
		}
		if (flag == null) {
			flushInsert(-1, resp);
			return false;
		}
		
		// 检查数据库表是否存在
		Space space = flag.getSpace();
		if (!Launcher.getInstance().existSpace(space)) {
			Logger.error("SQLPool.syncInsert, not found space '%s'", space);
			flushInsert(-1, resp);
			return false;
		}

		Logger.debug("SQLPool.syncInsert, flush to '%s'", space);
		long time = System.currentTimeMillis();
		
		// 数据写入本地磁盘
		int stamp = Install.insert(data);
		if(stamp < 1) { // 发生错误
			flushInsert(stamp, resp);
			return false;
		}
		
		// 根据日志发送更新数据到快点节点做备份
		byte[] log = Install.findReflexLog(stamp);
		
		Logger.debug("SQLPool.syncInsert, reflex log size:%d", (log == null ? -1 : log.length));
		
		int rows = 0;
		for (int seek = 0; seek < log.length;) {
			// 解析写入记录
			ReflexLog reflex = new ReflexLog();
			int len = reflex.resolve(log, seek, log.length - seek);
			seek += len;

			rows += reflex.getRows();
			space = reflex.getSpace();
			byte[] schema = space.getSchema().getBytes();
			byte[] table = space.getTable().getBytes();

			for (long cacheid : reflex.listCacheIdentity()) {
				// 获取缓存记录
				byte[] entity = Install.getCacheReflex(schema, table, cacheid);
				if (entity == null) continue;

				Logger.debug("SQLPool.syncInsert, cache entity id:%x, size:%d",
						cacheid, entity.length);

				// 数据传给CachePool，再分发给各备份节点做快照
				CachePool.getInstance().update(space, cacheid, entity);
			}
		}
		
		Logger.debug("SQLPool.syncInsert, flush '%s' to disk, insert count:%d, insert usedtime:%d",
				space, rows, System.currentTimeMillis() - time);

		// 返回结果
		flushInsert(rows, resp);
		return true;
	}

	/**
	 * 以异步的方式将数据写入磁盘 (数据先写入磁盘临时文件，再由线程调出写入磁盘数据块)。<br>
	 * 这种方式是大数据量的情况下，避免磁盘写缓慢的情况，堵塞后续操作
	 * 
	 * @param data
	 * @param resp
	 * @return
	 * @throws IOException
	 */
	private boolean asyncInsert(byte[] data, OutputStream resp) throws IOException {
		Logger.debug("SQLPool.asyncInsert, data len:%d", data.length);
		
//		int off = 0;
//		// all size
//		int allsize = Numeric.toInteger(data, off, 4);
//		off += 4;
//		if (data.length != allsize) {
//			flushInsert(-1, resp);
//			return false;
//		}
//		// version
//		int version = Numeric.toInteger(data, off, 4);
//		off += 4;
//		if (version != 1) {
//			flushInsert(-1, resp);
//			return false;
//		}
//		// space
//		int dbsize = data[off++];
//		int tbsize = data[off++];
//		
//		String db = new String(data, off, dbsize);
//		off += dbsize;
//		String table = new String(data, off, tbsize);
//		off += tbsize;
//		Space space = new Space(db, table);
		
		// 解析插入数据报头
		InsertFlag flag = null;
		DefaultInsert def = new DefaultInsert();
		try {
			flag = def.resolveFlag(data, 0, data.length);
		} catch (RuntimeException e) {
			Logger.error(e);
		} catch (Throwable e) {
			Logger.fatal(e);
		}
		if (flag == null) {
			flushInsert(-1, resp);
			return false;
		}
		
		// 检查数据库表名是否存在
		Space space = flag.getSpace();
		if (!Launcher.getInstance().existSpace(space)) {
			Logger.error("SQLPool.asyncInsert, cannot find space '%s'", space);
			flushInsert(-1, resp);
			return false;
		}
		
		// 写入异步缓冲池
		boolean success = StayPool.getInstance().write(data);
		// 返回结果
		flushInsert(success ? 1 : -1, resp);
		return success;
	}
	
	/**
	 * append data to lexst db
	 * @param data
	 * @param sync
	 * @param resp
	 * @return
	 * @throws IOException
	 */
	public boolean insert(byte[] data, boolean sync, OutputStream resp) throws IOException {
		if (sync) {
			return syncInsert(data, resp);
		} else {
			return asyncInsert(data, resp);
		}
	}
	

	
//	/**
//	 * 执行分布计算的 "DIRECT" 操作，输出计算结果的数据流(如SELECT检索数据)
//	 * @param direct
//	 * @param resp
//	 * @throws IOException
//	 */
//	public void direct(Direct direct, OutputStream resp) throws IOException {
//		int size = direct.getFrom().countSelect();
//		if (size > 0) {
//			if (size != 1) {
//				throw new IOException("direct from select size > 1");
//			}
//			direct_select(direct, resp);
//		} else {
//			direct_flush(null, direct, resp);
//		}
//	}

//	/**
//	 * 执行分布计算的"CONDUCT"操作，返回数据图谱的字节流
//	 * @param conduct
//	 * @param resp
//	 * @throws IOException
//	 */
//	public void conduct(Conduct conduct, OutputStream resp) throws IOException {
//		int size = conduct.getFrom().countSelect();
//		if (size > 0) {
//			if (size != 1) {
//				throw new IOException("from SELECT size > 1"); // error
//			}
//			conduct_select(conduct, resp);
//		} else {
//			conduct_flush(null, conduct, resp);
//		}
//	}
	

	
//	private void direct_select(Direct direct, OutputStream resp) throws IOException {
//		long time = System.currentTimeMillis();
//		
//		Select select = direct.getFrom().getSelect(0);
//		byte[] meta = select.build();
//
//		int stamp = Install.select(meta);
//		if (stamp < 0) {
//			flushDistributeResult(resp, Response.DISTRIBUTE_SERVERERR, Numeric.toBytes(stamp));
//			return;
//		} else if (stamp == 0) {
//			flushDistributeResult(resp, Response.DISTRIBUTE_FAILED, null);
//			return;
//		}
//
//		ByteArrayOutputStream buff = new ByteArrayOutputStream(0x500000);
//		while (true) {
//			byte[] b = Install.nextSelect(stamp, 0x100000);
//			if (b == null) break;
//			buff.write(b, 0, b.length);
//		}
//
//		Logger.debug("SQLPool.direct_select, data len:%d, query usedtime:%d", buff.size(), System.currentTimeMillis()-time);
//
//		direct_flush(buff, direct, resp);
//		
////		// jni query
////		long result = Install.select(meta);
////		int rows = (int) ((result >>> 32) & 0xffffffffL); //item count
////		int stamp = (int) (result & 0xffffffffL);	// task stamp identity
////
////		Logger.debug("SQLPool.dc_select, query count:%d, identity:%d", rows, stamp);
////
////		// read sql data
////		boolean finish = (rows == 0);
////		int readsize = 1048576;
////		ByteArrayOutputStream buff = new ByteArrayOutputStream(finish ? 16 : readsize);
////		while (!finish) {
////			byte[] b = Install.nextSelect(stamp, readsize);
////			// send to
////			if (b == null || b.length == 0) {
////				finish = true;
////			} else {
////				finish = (b[0] == 1);
////				buff.write(b, 1, b.length - 1);
////			}
////		}
////		
////		Logger.debug("SQLPool.dc_select, data len:%d, query usedtime:%d", buff.size(), System.currentTimeMillis()-time);
////
////		dc_flush(buff, dc, resp);
//	}

//	private void conduct_select(Conduct conduct, OutputStream resp) throws IOException {
//		long time = System.currentTimeMillis();
//		
//		// 只允许有一个SELECT，否则会引起混乱
//		Select select = conduct.getFrom().getSelect(0);
//		byte[] meta = select.build();
//		
//		int stamp = Install.select(meta);
//		if (stamp < 0) {
//			flushDistributeResult(resp, Response.DISTRIBUTE_SERVERERR, Numeric.toBytes(stamp));
//			return;
//		} else if (stamp == 0) {
//			flushDistributeResult(resp, Response.DISTRIBUTE_FAILED, null);
//			return;
//		}
//		
//		ByteArrayOutputStream buff = new ByteArrayOutputStream(0x500000);
//		while (true) {
//			byte[] b = Install.nextSelect(stamp, 0x100000);
//			if (b == null) break;
//			buff.write(b, 0, b.length);
//		}
//		
//		Logger.debug("SQLPool.conduct_select, data len:%d, query usedtime:%d", buff.size(), System.currentTimeMillis()-time);
//		
//		conduct_flush(buff, conduct, resp);
//
////		// jni query
////		long result = Install.select(query);
////		int rows = (int) ((result >>> 32) & 0xffffffffL); //item count
////		int stamp = (int) (result & 0xffffffffL);	// task stamp identity
////
////		Logger.debug("SQLPool.adc_select, query count:%d, identity:%d", rows, stamp);
////
////		// read sql data
////		boolean finish = (rows == 0);
////		int readsize = 1048576;
////		ByteArrayOutputStream buff = new ByteArrayOutputStream(finish ? 16 : readsize);
////		while (!finish) {
////			byte[] b = Install.nextSelect(stamp, readsize);
////			// send to
////			if (b == null || b.length == 0) {
////				finish = true;
////			} else {
////				finish = (b[0] == 1);
////				buff.write(b, 1, b.length - 1);
////			}
////		}
////		
////		Logger.debug("SQLPool.adc_select, data len:%d, query usedtime:%d", buff.size(), System.currentTimeMillis()-time);
////		
////		adc_flush(buff, adc, resp);
//	}
	
//	private void check() {
//		if (mapClient.isEmpty()) return;
//
//		long timeout = 120 * 1000;
//		ArrayList<SiteHost> excludes = new ArrayList<SiteHost>();
//
//		super.lockSingle();
//		try {
//			for (SiteHost host : mapClient.keySet()) {
//				ClientSet set = mapClient.get(host);
//				int size = set.size();
//				for (int i = 0; i < size; i++) {
//					WorkClient client = (WorkClient)set.get(i);
//					if (client.isRefreshTimeout(timeout)) {
//						set.remove(client);
//						client.stop();
//					}
//				}
//				if (set == null || set.isEmpty()) {
//					excludes.add(host);
//				}
//			}
//			for (SiteHost host : excludes) {
//				mapClient.remove(host);
//			}
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//	}
}