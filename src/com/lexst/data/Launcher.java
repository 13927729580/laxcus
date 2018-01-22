/**
 *
 */
package com.lexst.data;

import java.io.*;
import java.util.*;

import org.w3c.dom.*;

import com.lexst.algorithm.*;
import com.lexst.algorithm.disk.*;
import com.lexst.algorithm.from.*;
import com.lexst.data.effect.*;
import com.lexst.data.pool.*;
import com.lexst.log.client.*;
import com.lexst.remote.client.home.*;
import com.lexst.site.data.*;
import com.lexst.sql.*;
import com.lexst.sql.chunk.*;
import com.lexst.sql.index.range.*;
import com.lexst.sql.schema.*;
import com.lexst.thread.*;
import com.lexst.util.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;
import com.lexst.visit.*;
import com.lexst.visit.impl.data.*;
import com.lexst.xml.*;

/**
 * 数据节点启动器
 */
public class Launcher extends JobLauncher implements TaskEventListener {

	private static Launcher selfHandle = new Launcher();
	
	/** DATA节点本地配置 */
	private DataSite local = new DataSite();

	/** 数据库表名 -> 数据库配置 **/
	private LockMap<Space, Table> mapTable = new LockMap<Space, Table>();
	
	private IdentityPuddle puddle = new IdentityPuddle(); 
	// space catalog
	private SpacePuddle spaces = new SpacePuddle();
		
	// update space index identity
	private boolean updateModule;
	
	/**
	 * default constructor
	 */
	private Launcher() {
		super();
		super.setExitVM(true);
		streamImpl = new DataStreamInvoker();
		packetImpl = new DataPacketInvoker(fixpPacket);
		updateModule = false;
	}

	/**
	 * 返回data节点启动器静态句柄
	 * @return
	 */
	public static Launcher getInstance() {
		return Launcher.selfHandle;
	}
	
	/**
	 * RPC空调用
	 */
	public void nothing() {
		// none call
	}

	/**
	 * 返回当前节点级别(主节点或者从节点)
	 * @return
	 */
	public int getRank() {
		return local.getRank();
	}
	
	/**
	 * 返回当前DATA节点配置
	 * @return
	 */
	public DataSite getLocal() {
		return this.local;
	}
	

	/**
	 * 查找数据库表下的所有数据块信息
	 * @param space
	 * @return
	 */
	public ChunkStatus[] findChunk(Space space) {
		byte[] schema = space.getSchema().getBytes();
		byte[] table = space.getTable().getBytes();

		// 调用JNI，返回所在数据库下的全部数据块标识号
		long[] chunkIds = Install.getChunkIds(schema, table);
		if (chunkIds == null || chunkIds.length == 0) {
			Logger.warning("Launcher.findChunk, cannot find '%s' chunks", space);
			return null;
		}
		// 指取每个数据块资料
		ArrayList<ChunkStatus> array = new ArrayList<ChunkStatus>();
		for (long chunkid : chunkIds) {
			// 指取数据块的文件目录
			byte[] path = Install.findChunkPath(schema, table, chunkid);
			if (path == null || path.length == 0) {
				Logger.warning("Launcher.findChunk, cannot find '%s' - %x path", space, chunkid);
				continue;
			}
			String filename = new String(path);
			File file = new File(filename);
			if (file.exists() && file.isFile()) {
				long length = file.length();
				long modified = file.lastModified();
				ChunkStatus info = new ChunkStatus(chunkid, length, modified);
				array.add(info);
			}
		}
		int size = array.size();
		
		Logger.info("Launcher.findChunk, '%s' chunk size %d", space, size);
		
		if (size == 0) return null;
		ChunkStatus[] infos = new ChunkStatus[size];
		return array.toArray(infos);
	}
	
	/**
	 * 重构数据
	 * 
	 * @param space
	 * @return
	 */
	public boolean rebuild(Space space, short columnId) {
		Logger.debug("Launcher.rebuild, rebuild space '%s'", space);
		// when contains
		Table table = mapTable.get(space);
		if (table == null) {
			Logger.error("Launcher.rebuild, cannot find '%s'", space);
			return false;
		}
		return CommandPool.getInstance().deflate(table.getSpace(), columnId);
	}

	/**
	 * load index into memory
	 * @param space
	 * @return
	 */
	public boolean loadIndex(Space space) {
		Logger.debug("Launcher.loadIndex, load index '%s'", space);
		Table table = mapTable.get(space);
		if (table == null) {
			Logger.error("Launcher.loadIndex, cannot find '%s'", space);
			return false;
		}
		return CommandPool.getInstance().loadIndex(table.getSpace());
	}

	/**
	 * clear index from memory
	 * @param space
	 * @return
	 */
	public boolean stopIndex(Space space) {
		Logger.debug("Launcher.stopIndex, unload index '%s'", space);
		Table table = mapTable.get(space);
		if (table == null) {
			Logger.error("Launcher.stopIndex, cannot find '%s'", space);
			return false;
		}
		return CommandPool.getInstance().stopIndex(table.getSpace());
	}

	/**
	 * load chunk data to memory
	 * @param space
	 * @return
	 */
	public boolean loadChunk(Space space) {
		Logger.debug("Launcher.loadChunk, space '%s'", space);
		Table table = mapTable.get(space);
		if (table == null) {
			Logger.error("Launcher.loadChunk, cannot find '%s'", space);
			return false;
		}
		return CommandPool.getInstance().loadChunk(table.getSpace());
	}

	/**
	 * release chunk data from memory
	 * @param space
	 * @return
	 */
	public boolean stopChunk(Space space) {
		Logger.debug("Launcher.stopChunk, space '%s'", space);
		Table table = mapTable.get(space);
		if (table == null) {
			Logger.error("Launcher.stopChunk, cannot find '%s'", space);
			return false;
		}
		return CommandPool.getInstance().stopChunk(table.getSpace());
	}

	/**
	 * count chunk size
	 * @param path
	 * @return
	 */
	private long scanChunkDirectory(File path) {
		long filelen = 0L;
		if (path.exists() && path.isDirectory()) {
			File[] files = path.listFiles();
			for (int i = 0; files != null && i < files.length; i++) {
				if (files[i].exists()) {
					if (files[i].isFile()) filelen += files[i].length();
					else if (files[i].isDirectory()) filelen += scanChunkDirectory(files[i]);
				}
			}
		}
		return filelen;
	}
	
	/**
	 * count chunk size by schema
	 * @param schema
	 * @return
	 */
	public long showChunkSize(String schema) {
		Logger.debug("Launcher.showChunkSize, schema:%s", schema);
		
		ArrayList<Space> array = new ArrayList<Space>(mapTable.keySet());
		long filelen = 0L;
		for (Space space : array) {
			if (space.getSchema().equalsIgnoreCase(schema)) {
				filelen += showChunkSize(space.getSchema(), space.getTable());
			}
		}
		return filelen;
	}

	/**
	 * count chunk size by space
	 * @param schema
	 * @param table
	 * @return
	 */
	public long showChunkSize(String schema, String table) {
		Space space = new Space(schema, table);
		Logger.debug("Launcher.showChunkSize, space is:%s", space);
		
		if (!mapTable.containsKey(space)) return 0L;
		
		// get cache directory
		long filelen = 0L;
		byte[] path = Install.getCachePath(schema.getBytes(), table.getBytes());
		if (path != null && path.length > 0) {
			File dir = new File(new String(path));
			filelen += scanChunkDirectory(dir);
		}
		// get chunk directory
		for (int index = 0; true; index++) {
			path = Install.getChunkPath(schema.getBytes(), table.getBytes(), index);
			if (path == null || path.length == 0) break;
			File dir = new File(new String(path));
			filelen += scanChunkDirectory(dir);
		}
		return filelen;
	}

	/**
	 * find space table
	 * 
	 * @param space
	 * @return
	 */
	public Table findTable(Space space) {
		return mapTable.get(space);
	}

	/**
	 * load table
	 * @param client
	 * @return
	 */
	private boolean loadTable(HomeClient client) {
		boolean success = false;
		boolean nullable = (client == null);
		try {
			if (nullable) client = bring();
			for (Space space : spaces.list()) {
				Table table = client.findTable(space);
				if (table == null) {
					Logger.error("Launcher.loadTable, not found table '%s'", space);
					return false;
				}
				// 同时在本地和From任务管理池保存数据库表
				mapTable.put(space, table);
				FromTaskPool.getInstance().addTable(table);
				Logger.info("Launcher.loadTable, load table '%s'", space);
			}

			for (Naming naming : FromTaskPool.getInstance().getNamings()) {
				Project project = FromTaskPool.getInstance().findProject(naming);
				for(Space space : project.getSpaces()) {
					Table table = FromTaskPool.getInstance().findTable(space);
					if(table != null) continue;
					
//					Table table = mapTable.get(space);
//					if(table == null) {
//						table = client.findTable(space);
//					}
					
					// 向HOME节点查找数据库表
					table = client.findTable(space);
					// 如果没有找到配置，可能的解释是: 实际表已经删除，但是命名任务配置中没有删除表名
					if(table == null) {
						Logger.warning("Launcher.loadTable, cannot find table '%s'", space);
						continue;
					}
					// 保存数据库表
					FromTaskPool.getInstance().addTable(table);
					
//					project.setTable(space, table);
					Logger.info("Launcher.loadTable, load project table '%s'", space);
				}
			}
			
			success = true;
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			if (nullable) complete(client);
		}
		return success;
	}

	/**
	 * load jni service
	 * @param client
	 * @return
	 */
	private boolean loadJNI(HomeClient client) {
		//1. load table space
		for (Space space : mapTable.keySet()) {
			Table table = mapTable.get(space);
			boolean prime = local.isPrime(); //check site rank
			if (prime) {
				prime = table.isCaching(); //using cache? only prime site
			}
			// build to byte
			byte[] data = table.build();
			int ret = Install.initSpace(data, prime);
			boolean success = (ret >= 0);
			Logger.note(success, "Launcher.loadJNI, init '%s' result code %d", space, ret);
			if (!success) return false;
		}
		//2. check chunk id, when missing, add it
		int frees = Install.countFreeChunkIds();
		int size = mapTable.size();
		if (local.isPrime() && frees < size) {
			size = size - size % 10 + 10;
			long[] chunkIds = null;
			try {
				chunkIds = client.pullSingle(size);
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			if (chunkIds == null) {
				Logger.error("Launcher.loadJNI, cannot request chunkid!");
				return false;
			}
			for (int i = 0; i < chunkIds.length; i++) {
				Install.addChunkId(chunkIds[i]);
			}
		}
		//3. 进入JNI服务
		long time = System.currentTimeMillis();
		int ret = Install.launch();
		Logger.info("Launcher.loadJNI, launch used time %s, result code %d", System.currentTimeMillis() - time, ret);
		return ret == 0;
	}

	/**
	 * 停止JNI服务
	 */
	private void stopJNI() {
		Install.stop();
	}

	/**
	 * login to home site
	 * @param client
	 * @return
	 */
	private boolean login(HomeClient client) {
		Logger.info("Launcher.login, %s login to %s", local.getHost(), getHubSite());
		
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) {
			Logger.error("Launcher.login, cannot connect %s", getHubSite());
			return false;
		}

//		local.clearAllNaming();
//		local.addAllNaming(FromTaskPool.getInstance().listNaming());
		
		local.updateNamings(FromTaskPool.getInstance().getNamings());
		
		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.login(local);
				break;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			// when error, close
			client.close();
			delay(1000);
		}
		if(nullable) complete(client);
		return success;
	}

	/**
	 * relogn to home site
	 * @param site
	 * @return
	 */
	private boolean relogin() {
		Logger.info("Launcher.relogin, to home site %s", getHubSite());
		boolean success = false;
		HomeClient client = bring();
		if (client == null) {
			Logger.error("Launcher.relogin, cannot connect %s", getHubSite());
			return false;
		}

		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.relogin(local);
				break;
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			} 
			client.close();
			this.delay(1000);
		}
		complete(client);
		return success;
	}
	
	/**
	 * logout from home site
	 * @param client
	 * @return
	 */
	private boolean logout(HomeClient client) {
		Logger.info("Launcher.logout, %s from %s", local.getHost(), getHubSite());
		
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) {
			Logger.error("Launcher.logout, cannot connect %s", getHubSite());
			return false;
		}

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.logout(local.getFamily(), local.getHost());
				break;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			delay(1000);
		}
		if(nullable) complete(client);
		return success;
	}

	/**
	 * 启动DATA节点上的监控/管理池服务
	 * @return
	 */
	private boolean loadPool1() {
		// 设置本地主机地址
		DiskPool.getInstance().setLocal(local.getHost());
		// 设置更新监听器
		FromTaskPool.getInstance().setTaskEventListener(this);
		
		CachePool.getInstance().start();
		ChunkPool.getInstance().start();
		DiskPool.getInstance().start();
		
		StayPool.getInstance().start();
		
		// 启动diffuse任务热发布任务管理池(diffuse只位于DATA节点)
		FromTaskPool.getInstance().start();
		// 启动conduct分布计算
		ConductPool.getInstance().start();
		// 启动SQL计算
		boolean success = SQLPool.getInstance().start();
		
		Logger.note("Launcher.loadPool1, load sql pool", success);
		if (success) {
			success = CommandPool.getInstance().start();
			Logger.note("Launcher.loadPool1, load command pool", success);
		}
		return success;
	}

	/**
	 * 启动管理池(区分DATA节点的层级分别启动)
	 * @return
	 */
	private boolean loadPool2() {
		boolean success = false;
		if (local.isPrime()) {
			success = PrimePool.getInstance().start();
			Logger.note("Launcher.loadPool2, load prime pool", success);
			if (success) {
				success = UpdatePool.getInstance().start();
				Logger.note("Launcher.loadPool2, load update pool", success);
				if(!success) {
					PrimePool.getInstance().stop();
				}
			}
		} else {
			success = SlavePool.getInstance().start();
			Logger.note("Launcher.loadPool2, load slave pool", success);
		}
		return success;
	}

	/**
	 * 关闭监控/管理池
	 */
	private void stopPool() {
		StayPool.getInstance().stop();
		while (StayPool.getInstance().isRunning()) {
			this.delay(200);
		}

		UpdatePool.getInstance().stop();
		PrimePool.getInstance().stop();
		SlavePool.getInstance().stop();
		SQLPool.getInstance().stop();
		ConductPool.getInstance().stop();
		
		FromTaskPool.getInstance().stop();
		CommandPool.getInstance().stop();
		ChunkPool.getInstance().stop();
		CachePool.getInstance().stop();
		DiskPool.getInstance().stop();
		
		while(ConductPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (SQLPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (FromTaskPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (PrimePool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (SlavePool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (UpdatePool.getInstance().isRunning()) {
			this.delay(500);
		}
		while(CommandPool.getInstance().isRunning()) {
			this.delay(500);
		}
		while(CachePool.getInstance().isRunning()) {
			this.delay(500);
		}
		while(ChunkPool.getInstance().isRunning()) {
			this.delay(500);
		}
		while(DiskPool.getInstance().isRunning()) {
			this.delay(500);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		//1. 从HOME节点注销
		this.logout(null);
		//2. 停止池服务
		this.stopPool();
		//3. 停止FIXP监听服务
		super.stopListen();
		//4. 写数据库表配置信息到磁盘
		this.flushSpace();
		//5. flush chunk num configure
		this.flushIdentity();
		// 6. flush cache entity
		this.flushEntity();
		//6. 关闭JNI接口服务
		this.stopJNI();
		//7. 关闭日志服务
		super.stopLog();
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// 连接HOME节点
		HomeClient client = bring();
		if (client == null) {
			Logger.error("Launcher.init, cannot find home site %s", getHubSite());
			return false;
		}
		
		//1. 加载并且启动日志服务
		boolean	success = loadLog(local.getFamily(), client);
		Logger.note("Launcher.init, load log", success);
		//2. 加载并且设置DATA节点超时时间
		if (success) {
			success = loadTimeout(local.getFamily(), client);
			Logger.note(success, "Launcher.init, site timeout %d", getSiteTimeout());
			if (!success) stopLog();
		}
		//3. 设置系统时间，与HOME节点时间误差小于1秒
		if (success) {
			super.loadTime(client);
		}
		//4. 启动FIXP监听服务器
		if (success) {
			Class<?>[] clazz = { DataVisitImpl.class };
			success = loadListen(clazz, local.getHost());
			Logger.note("Launcher.init, load listen", success);
			if (!success) stopLog();
		}
		//5. 加载本地的数据库表 
		if (success) {
			success = loadTable(client);
			Logger.note("Launcher.init, load table", success);
			if (!success) {
				stopListen();
				stopLog();
			}
		}
		//6. 启动本地JNI服务(c and c++)
		if (success) {
			success = loadJNI(client);
			Logger.note("Launcher.init, load JNI service", success);
			if (!success) {
				stopListen();
				stopLog();
			}
		}
		//7. 加载本地磁盘数据的索引映象
		if(success) {
			success = loadIndex();
			Logger.note("Launcher.init, load index", success);
			if (!success) {
				stopJNI();
				stopListen();
				stopLog();
			}
		}
		//8. 启动管理池服务
		if(success) {
			success = loadPool1();
			Logger.note("Launcher.init, load pool1", success);
			if(!success) {
				stopJNI();
				stopListen();
				stopLog();
			}
		}
		//9. 注册到HOME节点
		if (success) {
			success = login(client);
			Logger.note("Launcher.init, login", success);
			if (!success) {
				stopPool();
				stopJNI();
				stopListen();
				stopLog();
			}
		}
		// 关闭HOME连接
		complete(client);
		//10. choose load rank pool
		if(success) {
			success = loadPool2();
			Logger.note("Launcher.init, load pool2", success);
			if(!success) {
				this.finish();
			}
		}
		
		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("Launcher.process, site into ...");
		refreshEndTime();
		
		while (!isInterrupted()) {
			long end = System.currentTimeMillis() + 1000;
			
			if(super.isLoginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.login(null);
			} else if (super.isReloginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.relogin();
			} else if (isMaxSiteTimeout()) {
				Logger.warning("Launcher.process, max site timout!");
				this.refreshEndTime();
				this.relogin();
			} else if (isSiteTimeout()) {
				this.hello(local.getFamily(), getHubSite()); // active to home
			}

			//1. check chunkid(only prime site)
			this.checkMissing();
			// when update space, update index and relogin to home site
			if (updateModule) {
				Logger.info("Launcher.process, update data space");
				setUpdateModule(false);
				boolean success = loadIndex();
				if(success) this.relogin();
			}

			long timeout = end - System.currentTimeMillis();
			if (timeout > 0) delay(timeout);
		}
		Logger.info("Launcher.process, site exit");
	}

	/**
	 * check chunkid, when missing, add it
	 */
	private void checkMissing() {
		if (!local.isPrime()) return;
		// call jni
		int count = Install.countFreeChunkIds();
		if (count > 3) return;

		// apply chunkid from top site
		long[] chunkIds = this.applyChunkId(10);
		int size = (chunkIds == null ? 0 : chunkIds.length);
		// add chunkid to jni
		Logger.info("Launcher.checkMissing, new chunkid count %d", size);
		if (size > 0) {
			for (int i = 0; i < size; i++) {
				Install.addChunkId(chunkIds[i]);
			}
		}
	}

	/**
	 * 加载所有数据表的所有数据块的索引映象
	 * @return
	 */
	private boolean loadIndex() {
		// 清除旧记录
		local.clear();
		// 获取当前磁盘空间尺寸(自由空间和已经使用的空间)
		long[] sizes = Install.getDiskSpace();
		if (sizes == null || sizes.length != 2) {
			Logger.error("Launcher.loadIndex, cannot get disk size");
			return false;
		}

		local.setFree(sizes[0]);
		local.setUsable(sizes[1]);
		Logger.info("Launcher.loadIndex, disk free size [%d - %dM], used size [%d - %dM]",
				local.getFree(), local.getFree() / 1024 / 1024, local.getUsable(), local.getUsable() / 1024 / 1024);

		// table space is none, exit
		if (spaces.isEmpty()) return true;
		// 获取表下所有数据块索引映象
		byte[] bytes = Install.pullChunkIndex();
		Logger.info("Launcher.loadIndex, index module size %d", (bytes == null ? -1 : bytes.length));
		if (bytes != null && bytes.length > 0) {
			boolean success = splitIndex(bytes);
			Logger.note("Launcher.loadIndex, split index", success);
			if (!success) {
				return false;
			}
		}
		// padding missing space
		IndexSchema schema = local.getIndexSchema();
		for (Space space : spaces.list()) {
			if (!schema.contains(space)) {
				schema.add(space);
			}
		}
		return true;
	}

	/**
	 * split c++ data to java
	 * @param bytes
	 * @return
	 */
	private boolean splitIndex(byte[] bytes) {
		// 字节长度小于12是错误
		if (bytes.length < 12) {
			return false;
		}
		
		IndexSchema indexSchema = local.getIndexSchema();
		int seek = 0;
		// all byte size
		int datalen = Numeric.toInteger(bytes, seek, 4); seek += 4;
		// chunk count
		int allchunk = Numeric.toInteger(bytes, seek, 4); seek += 4;
		// version
		int version = Numeric.toInteger(bytes, seek, 4); seek += 4;
		// check error
		if (bytes.length - 12 != datalen || version != 1) {
			return false;
		}

		int chunks = 0;
		while(seek < bytes.length) {
			if (seek + 2 > bytes.length) return false;
			// table space
			int dbsz = bytes[seek++] & 0xff;
			int tbsz = bytes[seek++] & 0xff;
			if (seek + dbsz + tbsz > bytes.length) {
				return false;
			}
			String db = new String(bytes, seek, dbsz); seek += dbsz;
			String table = new String(bytes, seek, tbsz); seek += tbsz;
			Space space = new Space(db, table);

			if (seek + 8 > bytes.length) return false;
			// all chunk byte size by space
			int chunklen = Numeric.toInteger(bytes, seek, 4); seek += 4;
			// chunk count by space
			int chunksum = Numeric.toInteger(bytes, seek, 4); seek += 4;

			Logger.info("Launcher.splitIndex, space '%s', chunk len: %d, chunk count: %d", space, chunklen, chunksum);

			int elements = chunks;
			for (int pos = seek; seek - pos < chunklen;) {
				if (seek + 16 > bytes.length) return false;
				// chunk id
				long chunkId = Numeric.toLong(bytes, seek, 8); seek += 8;
				// chunk rank(prime or slave)
				byte rank = bytes[seek++];
				// chunk status(incomplete or complete)
				byte status = bytes[seek++];
				// byte size of chunk size and index count
				int idxsize = Numeric.toInteger(bytes, seek, 4); seek += 4;
				short idxcount = Numeric.toShort(bytes, seek, 2); seek += 2;
				
				Logger.info("Launcher.splitIndex, chunk %x, rank %d, status %d, index size %d, index count %d",
						chunkId, rank, status, idxsize, idxcount);

				ChunkAttribute sheet = new ChunkAttribute(chunkId, rank, status);
				int chunkoff = seek;
				for(short i = 0; i < idxcount; i++) {
					IndexRange index = null;
					if (seek + 3 > bytes.length) return false;
					byte type = bytes[seek++];
					short columnId = Numeric.toShort(bytes, seek, 2); seek += 2;
					
					Logger.info("Launcher.splitIndex, columnId:%d - type:%d", columnId, type);
					
					// check type and build index
					if (type == Type.SHORT_INDEX) {
						if(seek + 4 > bytes.length) return false;
						short begin = Numeric.toShort(bytes, seek, 2); seek += 2;
						short end = Numeric.toShort(bytes, seek, 2); seek += 2;
						index = new ShortIndexRange(chunkId, columnId, begin, end);
					} else if (type == Type.INTEGER_INDEX) {
						if (seek + 8 > bytes.length) return false;
						int begin = Numeric.toInteger(bytes, seek, 4); seek += 4;
						int end = Numeric.toInteger(bytes, seek, 4); seek += 4;
						index = new IntegerIndexRange(chunkId, columnId, begin, end);
					} else if (type == Type.LONG_INDEX) {
						if(seek + 16 > bytes.length) return false;
						long begin = Numeric.toLong(bytes, seek, 8); seek += 8;
						long end = Numeric.toLong(bytes, seek, 8); seek += 8;
						index = new LongIndexRange(chunkId, columnId, begin, end);
					} else if (type == Type.FLOAT_INDEX) {
						if (seek + 8 > bytes.length) return false;
						int begin = Numeric.toInteger(bytes, seek, 4); seek += 4;
						int end = Numeric.toInteger(bytes, seek, 4); seek += 4;
						index = new FloatIndexRange(chunkId, columnId,
								Float.intBitsToFloat(begin), Float.intBitsToFloat(end));
					} else if (type == Type.DOUBLE_INDEX) {
						if(seek + 16 > bytes.length) return false;
						long begin = Numeric.toLong(bytes, seek, 8);
						seek += 8;
						long end = Numeric.toLong(bytes, seek, 8);
						seek += 8;
						index = new DoubleIndexRange(chunkId, columnId,
								Double.longBitsToDouble(begin), Double.longBitsToDouble(end));
					}
					if (index == null) {
						Logger.error("Launcher.splitIndex, invalid index '%s'", space);
						return false;
					}
					sheet.add(index);
				}
				if (seek - chunkoff != idxsize) {
					Logger.error("Launcher.splitIndex, invalid chunk size!");
					return false;
				}
				// save chunk
				indexSchema.add(space, sheet);
				chunks++;
			}
			if(chunks - elements != chunksum) {
				Logger.error("Launcher.splitIndex, invalid chunk num '%s'", space);
				return false;
			}
		}
		if(chunks != allchunk) {
			Logger.error("Launcher.splitIndex, invalid chunk num!");
			return false;
		}
		return true;
	}

	/**
	 * find index table
	 * @param space
	 * @return
	 */
	public IndexTable findIndex(Space space) {
		IndexSchema schema = local.getIndexSchema();
		return schema.find(space);
	}

	/**
	 * 
	 * @param b
	 */
	public synchronized void setUpdateModule(boolean b) {
		this.updateModule = b;
	}

	/**
	 * @param table
	 * @return
	 */
	public boolean createSpace(Table table) {
		Space space = table.getSpace();
		Logger.info("Launcher.createSpace, create table space '%s'", space);
		
		// when prime site, check chunkid num
		boolean prime = local.isPrime();
		if(prime) {
			int cid = Install.countFreeChunkIds();
			Logger.info("Launcher.createSpace, count chunk id is:%d", cid);
			while (cid == 0) {
				if (puddle.isEmpty()) {
					long[] chunkIds = applyChunkId(10);
					puddle.add(chunkIds);
					Logger.info("Launcher.createSpace, count apply chunk identity %d", (chunkIds == null ? 0 : chunkIds.length));
				} else {
					while (true) {
						long chunkId = puddle.poll();
						if (chunkId == 0L) break;
						Install.addChunkId(chunkId);
					}
					break;
				}
			}
		}
		// when prime site, check cache mode
		if (prime) {
			prime = table.isCaching();
		}
		// call jni, create table space
		byte[] data = table.build();
		int ret = Install.createSpace(data, prime);
		boolean success = (ret >= 0);
		Logger.note(success, "Launcher.createSpace, create table space '%s' result code %d", space, ret);
		if (success) {
			// write xml configure file
			spaces.add(space);
			// 向本地和From任务池写入数据库表
			mapTable.put(space, table);
			FromTaskPool.getInstance().addTable(table);
			flushSpace();
			// set refresh space status to true
			setUpdateModule(true);
		}
		return success;
	}

	/**
	 * 删除数据库存储空间
	 * @param db
	 * @param table
	 * @return
	 */
	public boolean deleteSpace(Space space) {
		Logger.info("Launcher.deleteSpace, delete table space '%s'", space);
		// 根据表名删除存储空间
		byte[] db = space.getSchema().getBytes();
		byte[] table = space.getTable().getBytes();
		int ret = Install.deleteSpace(db, table);
		// 非0即错误
		if (ret != 0) {
			Logger.info("Launcher.deleteSpace, cannot delete table space '%s'", space);
			return false;
		}
		Logger.info("Launcher.deleteSpace, delete table space '%s' success", space);
		// 删除成功，重新磁盘上的数据配置 
		spaces.remove(space);
		// 删除本地内存和From任务池上的数据库表
		mapTable.remove(space);
		FromTaskPool.getInstance().removeTable(space);
		flushSpace();
		setUpdateModule(true);
		return true;
	}

	public boolean existSpace(Space space) {
		return spaces.exists(space);
	}
	
	public List<Space> listSpace() {
		return spaces.list();
	}

	/*
	 * 接受FromTaskPool通知，当前命名集合已经改变，更新全部命名记录
	 * @see com.lexst.algorithm.TaskEventListener#updateNaming()
	 */
	@Override
	public void updateNaming() {
		// 提取更新后的命名
		Set<Naming> set = FromTaskPool.getInstance().getNamings();
		// 更新全部diffuse命名
		local.updateNamings(set);
		// 重新注册到HOME服务器
		setOperate(BasicLauncher.RELOGIN);
	}

	/**
	 * request chunk id, from home site
	 * @return
	 */
	public long[] applyChunkId(int num) {
		if(num < 10) num = 10;
		HomeClient client = bring();
		long[] chunkIds = null;
		try {
			chunkIds = client.pullSingle(num);
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.error(exp);
		}
		complete(client);
		return chunkIds;
	}

	/**
	 * load configure file
	 * @param filename
	 * @return
	 */
	private boolean loadLocal(String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) {
			return false;
		}

		Logger.info("Launcher.loadLocal, load xml resource");

		// 解析并且保留HOME节点地址
		SiteHost host = splitHome(document);
		if(host == null) {
			Logger.error("Launcher.loadLocal, split home failed");
			return false;
		}
		super.setHubSite(host);

		// 解析并且保存本地节点地址
		host = super.splitLocal(document);
		if(host == null) {
			Logger.error("Launcher.loadLocal, split local failed!");
			return false;
		}
		local.setHost(host);
		
		// 加载远程关闭配置
		if (!loadShutdown(document)) {
			Logger.error("Launcher.loadLocal, split shutdown failed!");
			return false;
		}

		Logger.info("Launcher.loadLocal, load local resource");

		// data节点资源配置目录(如拥有的数据库表等)
		String s = xml.getXMLValue(document.getElementsByTagName("resource-directory"));
		if (!super.createResourcePath(s)) {
			Logger.error("Launcher.loadLocal, cannot create path %s", s);
			return false;
		}
		
		// 解析并且加载FIXP安全管理配置
		if(!super.loadSecurity(document)) {
			Logger.error("Launcher.loadLocal, cannot resolve security file");
			return false;
		}

		// 异步存储目录(insert分两种，同步和异步。同步直接写入数据库，异步先写临时文件再写入数据库)
		s = xml.getXMLValue(document.getElementsByTagName("async-store-directory"));
		if (s != null && s.length() > 0) {
			StayPool.getInstance().createTempPath(s);
		}

		// conduct计算结果数据的临时存储目录
		s = xml.getXMLValue(document.getElementsByTagName("conduct-directory"));
		if (s != null && s.length() > 0) {
			DiskPool.getInstance().setRoot(s);
		}

		// 设置diffuse任务热发布管理目录
		s = xml.getXMLValue(document.getElementsByTagName("diffuse-root"));
		if (s != null && s.length() > 0) {
			FromTaskPool.getInstance().setRoot(s);
		}

		// 存储数据目录
		Element element = (Element) document.getElementsByTagName("chunk-directory").item(0);
		String build = xml.getValue(element, "build");
		String cache = xml.getValue(element, "cache");
		String[] paths = xml.getXMLValues(element.getElementsByTagName("store"));
		int ret = Install.setBuildRoot(build.getBytes());
		Logger.note(ret == 0, "Launcher.loadLocal, load build path %s", build);
		if (ret != 0) return false;
		ret = Install.setCacheRoot(cache.getBytes());
		Logger.note(ret == 0, "Launcher.loadLocal, load cache path %s", cache);
		if (ret != 0) return false;
		for (String path : paths) {
			ret = Install.setChunkRoot(path.getBytes());
			Logger.note(ret == 0, "Launcher.loadLocal, load store path %s", path);
			if (ret != 0) return false;
		}

		// data节点级别(主节点或者从节点)
		s = xml.getXMLValue(document.getElementsByTagName("rank"));
		byte rank = 0;
		if ("master".equalsIgnoreCase(s) || "prime".equalsIgnoreCase(s)) {
			rank = DataSite.PRIME_SITE;
		} else if("slave".equalsIgnoreCase(s)) {
			rank = DataSite.SLAVE_SITE;
		}
		Logger.info("Launcher.loadLocal, site rank '%s - %d'", s, rank);
		if (rank == 0) {
			Logger.error("Launcher.loadLocal, unknown site rank");
			return false;
		}
		// 设置当前节点级别(主节点或者从节点)
		local.setRank(rank);
		Install.setRank(rank);

		// JNI 允许的最大工作线程
		s = xml.getXMLValue(document.getElementsByTagName("job-threads"));
		int threads = Integer.parseInt(s);
		Install.setWorker(threads);

		Logger.info("Launcher.loadLocal, job threads is %d", threads);
		
		// 加载日志配置
		return Logger.loadXML(filename);
	}

	/**
	 * load space data from disk
	 * @return
	 */
	private boolean loadSpace() {
		File file = buildResourceFile(SpacePuddle.filename);
		// not found file, return true
		if (!file.exists()) return true;
		byte[] data = readFile(file);
		if(data == null) return false;
		return spaces.parseXML(data);
	}

	/**
	 * 写数据库表配置到磁盘上
	 * @return
	 */
	private boolean flushSpace() {
		byte[] bytes = spaces.buildXML();
		// flush to disk
		File file = buildResourceFile(SpacePuddle.filename);
		return flushFile(file, bytes);
	}

	/**
	 * 从磁盘读取并且输入数据块标识号到JNI
	 * @return
	 */
	private boolean loadIdentity() {
		File file = buildResourceFile(IdentityPuddle.filename);
		if(!file.exists()) return true;
		byte[] data = readFile(file);
		if(data == null) return false;
		puddle.parseXML(data);
		// flush chunk id to jni
		int count = 0;
		while (true) {
			long chunkId = puddle.poll();
			if (chunkId == 0L) break;
			int ret = Install.addChunkId(chunkId);
			if(ret == 0) count++;
		}
		Logger.info("Launcher.loadIdentity, load count %d", count);
		return true;
	}

	/**
	 * 输出JNI接口中未使用的数据块标识号
	 * 
	 * @return
	 */
	private boolean flushIdentity() {
		long[] ids = Install.getFreeChunkIds();
		
		Logger.info("Launcher.flushIdentity, chunk id count %d", (ids == null ? -1 : ids.length));
		
		for (int i = 0; ids != null && i < ids.length; i++) {
			puddle.add(ids[i]);
		}
		byte[] data = puddle.buildXML();
		// flush to disk
		File file = buildResourceFile(IdentityPuddle.filename);
		return flushFile(file, data);
	}
	
	private boolean loadEntity() {
		File file = buildResourceFile("cache_entity.xml");
		if(!file.exists()) return true;
		
		byte[] data = readFile(file);
		
		return CachePool.getInstance().resolve(data);
	}
	
	private boolean flushEntity() {
		byte[] data = CachePool.getInstance().build();
		File file = buildResourceFile("cache_entity.xml");
		return flushFile(file, data);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args == null || args.length < 1) {
			Logger.error("parameters missing!");
			Logger.gushing();
			return;
		}
		// 初始化JNI接口
		int ret = Install.initialize();
		if(ret != 0) {
			Logger.error("initialize failed, program will exit!");
			Logger.gushing();
			return;
		}
		// 解析并且设置本地资源配置
		String filename = args[0];
		boolean success = Launcher.getInstance().loadLocal(filename);
		Logger.note("Launcher.main, load local", success);
		if (success) {
			success = Launcher.getInstance().loadSpace();
			Logger.note("Launcher.main, load space", success);
		}
		// load chunk identity
		if (success) {
			success = Launcher.getInstance().loadIdentity();
			Logger.note("Launcher.main, load chunk identity", success);
		}
		// load cache entity
		if(success) {
			success = Launcher.getInstance().loadEntity();
			Logger.note("Launcher.main, load cache entity", success);
		}
		// start service
		if (success) {
			success = Launcher.getInstance().start();
			Logger.note("Launcher.main, start service", success);
		}
		if(!success) {
			Logger.gushing();
		}
	}
		
}