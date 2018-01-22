/**
 * 
 */
package com.lexst.build;

import java.io.*;
import java.util.*;

import org.w3c.dom.*;

import com.lexst.algorithm.*;
import com.lexst.algorithm.build.*;
import com.lexst.build.effect.*;
import com.lexst.data.*;
import com.lexst.data.effect.*;
import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.remote.client.build.*;
import com.lexst.remote.client.home.*;
import com.lexst.site.build.*;
import com.lexst.sql.chunk.*;
import com.lexst.sql.schema.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.visit.impl.build.*;
import com.lexst.xml.*;
import com.lexst.util.naming.*;

/**
 * 数据重组节点启动器，实现用户自定义的ETL服务。<br>
 * ETL: extract, transform, load <br> 
 *
 */
public class Launcher extends JobLauncher implements BuildChooser, TaskEventListener {
	
	private static Launcher selfHandle = new Launcher();
	
	/** 本地节点地址 **/
	private BuildSite local = new BuildSite();

	/** Build任务命名 -> 运行状态的实例句柄 **/
	private Map<Naming, BuildTask> mapTask = new HashMap<Naming, BuildTask>();


	private FreeIdPuddle puddle = new FreeIdPuddle();
		
	/**
	 * 
	 */
	private Launcher() {
		super();
		super.setExitVM(true);
		streamImpl = new BuildStreamInvoker();
		packetImpl = new BuildPacketInvoker(fixpPacket);
	}
	
	/**
	 * @return
	 */
	public static Launcher getInstance() {
		return Launcher.selfHandle;
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.build.task.BuildInbox#getHome()
	 */
	@Override
	public SiteHost getHome() {
		return getHubSite();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.build.task.BuildInbox#getLocal()
	 */
	@Override
	public BuildSite getLocal() {
		return this.local;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.build.task.BuildInbox#setLogin(boolean)
	 */
	@Override
	public void setLogin(boolean b) {
		this.setOperate(BasicLauncher.RELOGIN);
	}

	/*
	 * Build任务发来通知，要求删除自己(在最后完成时发此通知)
	 * 
	 * @see com.lexst.build.task.BuildInbox#removeTask(com.lexst.util.naming.Naming)
	 */
	@Override
	public boolean removeTask(Naming naming) {
		super.lockSingle();
		try {
			return mapTask.remove(naming) != null;
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
		return false;
	}

	/**
	 * empty call
	 */
	public void nothing() {
		
	}
	
	/**
	 * 根据命名找到对应任务实例，执行数据重构(ETL服务)
	 * @param name
	 * @return
	 */
	public boolean execute(String name) {
		Logger.info("Launcher.execute, naming is %s", name);
		
		// 根据命名查找匹配的任务实例
		Naming naming = new Naming(name);
		BuildTask task = BuildTaskPool.getInstance().find(naming);
		boolean success = (task != null);
		if (!success) {
			Logger.error("Launcher.execute, cannot find %s", naming);
			return false;
		}

		// 保存任务句柄
		success = false;
		super.lockSingle();
		try {
			if (mapTask.containsKey(naming)) {
				Logger.error("Launcher.execute, %s is running!", naming);
			} else {
				mapTask.put(naming, task);
				success = true;
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
		if (!success) {
			return false;
		}
		
		// 数据重构(marshal/educe, ETL)
		success = false;
		try {
			success = task.rebuild();
		} catch (BuildTaskException e) {
			Logger.error(e);
		}
		
		// 数据转换不成功，删除句柄
		if (!success) {
			super.lockSingle();
			try {
				mapTask.remove(naming);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			} finally {
				super.unlockSingle();
			}
		}
		return success;
	}

	/**
	 * 检查某个表是否在重构状态
	 * @param space
	 * @return
	 */
	public boolean isBuilding(Space space) {
		Logger.info("Launcher.isBuilding, space is '%s'", space);
		
		super.lockMulti();
		try {
			for (Naming naming : mapTask.keySet()) {
				BuildTask task = mapTask.get(naming);
				Project project = task.getProject();
				if (project.getSpaces().contains(space)) {
					return true;
				}
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return false;
	}

	/**
	 * call jni, get chunk information 
	 * @param space
	 * @return
	 */
	public ChunkStatus[] findChunkInfo(Space space) {
		byte[] db = space.getSchema().getBytes();
		byte[] table = space.getTable().getBytes();

		// call jni, get all chunkid
		long[] chunkIds = Install.getChunkIds(db, table);
		if (chunkIds == null || chunkIds.length == 0) {
			Logger.warning("Launcher.findChunkInfo, cannot find '%s' chunk", space);
			return null;
		}
		// get chunk information
		ArrayList<ChunkStatus> array = new ArrayList<ChunkStatus>();
		for (long chunkid : chunkIds) {
			// find chunk filename
			byte[] path = Install.findChunkPath(db, table, chunkid);
			if (path == null || path.length == 0) {
				Logger.warning("Launcher.findChunkInfo, cannot find '%s' - %x path", space, chunkid);
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
		
		Logger.info("Launcher.findChunkInfo, '%s' chunk size %d", space, size);
		
		if (size == 0) return null;
		ChunkStatus[] infos = new ChunkStatus[size];
		return array.toArray(infos);
	}

	/**
	 * 注册到HOME节点
	 * @param client
	 * @return
	 */
	private boolean login(HomeClient client) {
		Logger.info("Launcher.login, %s to %s", local.getHost(), getHubSite());
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) {
			Logger.error("Launcher.login, cannot connect %s", getHubSite());
			return false;
		}

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.login(local);
				break;
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(1000);
		}
		if (nullable) complete(client);
		return success;
	}
	
	/**
	 * 从HOME节点注销
	 * @param client
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
		if (nullable) complete(client);
		return success;
	}
	
	/**
	 * relogin site
	 * @param client
	 * @return
	 */
	private boolean relogin(HomeClient client) {
		Logger.info("Launcher.relogin, %s to %s", local.getHost(), getHubSite());
		
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) {
			Logger.error("Launcher.relogin, cannot connect %s", getHubSite());
			return false;
		}

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.relogin(local);
				break;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(5000);
		}
		if (nullable) complete(client);
		return success;
	}

	/**
	 * 启动JNI服务
	 */
	private boolean loadJNI() {
		int ret = Install.launch();
		return (ret == 0);
	}

	/**
	 * 停止JNI服务
	 */
	private void stopJNI() {
		// 提取未使用的数据块标识号，写入本地磁盘
		long[] chunkIds = Install.getFreeChunkIds();
		for (int i = 0; chunkIds != null && i < chunkIds.length; i++) {
			puddle.add(chunkIds[i]);
		}
		this.flushIdentity();
		// 停止JNI
		Install.stop();
	}
	
	/**
	 * 加载资源管理池服务
	 * 
	 * @return
	 */
	private boolean loadPool() {
		Logger.info("Launcher.loadPool, loading ...");
		BuildTaskPool.getInstance().setTaskEventListener(this);
		BuildTaskPool.getInstance().setBuildChooser(this);
		return BuildTaskPool.getInstance().start();
	}

	/**
	 * 停止资源管理池服务
	 */
	private void stopPool() {
		Logger.info("Launcher.stopPool, stop all...");
		BuildTaskPool.getInstance().stop();
		while (BuildTaskPool.getInstance().isRunning()) {
			this.delay(200);
		}
	}
	
	/**
	 * 从HOME节点加载数据库表，保存到Build任务管理器
	 * @param client
	 * @return
	 */
	private boolean loadTable(HomeClient client) {
		boolean success = false;
		boolean nullable = (client == null);
		try {
			if (nullable) client = bring();			
			for (Naming naming : BuildTaskPool.getInstance().getNamings()) {
				Project project = BuildTaskPool.getInstance().findProject(naming);
				for (Space space : project.getSpaces()) {
					Table table = client.findTable(space);
					if (table == null) {
						Logger.error("Launcher.loadTable, cannot find table '%s'", space);
						return false;
					}
					BuildTaskPool.getInstance().addTable(table);
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
	 * 停止全部运行的ETL任务
	 */
	private void stopTask() {
		// 停止处于运行状态的命名任务
		try {
			for (BuildTask task : mapTask.values()) {
				task.halt();
			}
		} catch (BuildTaskException e) {
			Logger.error(e);
		} catch (Throwable t) {
			Logger.fatal(t);
		}

		// wait...
		while (!mapTask.isEmpty()) {
			this.delay(500);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// 所有BUILD节点都是从节点
		Install.setRank(BuildSite.SLAVE_SITE);
		// 连接HOME节点
		HomeClient client = bring();
		if (client == null) {
			Logger.error("Launcher.init, cannot connect home %s", super.getHubSite());
			return false;
		}
		
		// 1. 启动日志服务
		boolean success = loadLog(local.getFamily(), client);
		Logger.note("Launcher.init, load log", success);
		// 2. 设置BUILD节点超时时间
		if (success) {
			success = loadTimeout(local.getFamily(), client);
			Logger.note(success, "Launcher.init, set site timeout %d", getSiteTimeout());
			if (!success) stopLog();
		}
		// 3. 设置系统时间，与HOME节点误差不超过一秒
		if (success) {
			super.loadTime(client);
		}
		//4. 启动FIXP监听器
		if (success) {
			Class<?>[] clazz = { BuildVisitImpl.class };
			success = loadListen(clazz, local.getHost());
			Logger.note("Launcher.init, load listen", success);
			if (!success) stopLog();
		}
		//5. 加载管理池服务
		if (success) {
			success = loadPool();
			Logger.note("Launcher.init, load pool", success);
			if (!success) stopLog();
		}
		//6. 加载数据库表
		if(success) {
			success = loadTable(client);
			Logger.note("Launcher.init, load table", success);
			if(!success) {
				stopListen();
				stopLog();
				stopPool();
			}
		}
		//7. 启动JNI服务
		if(success) {
			success = loadJNI();
			Logger.note("Launcher.init, load JNI", success);
			if (!success) {
				stopListen();
				stopLog();
			}
		}
		//8. 注册到HOME节点
		if (success) {
			this.updateNaming();
			success = login(client);
			Logger.note("Launcher.init, login", success);
			if (!success) {
				stopTask();
				stopJNI();
				stopListen();
				stopLog();
			}
		}
		// 关闭连接
		complete(client);
		
		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("Launcher.process, site into...");
		this.refreshEndTime();
		long time2 = System.currentTimeMillis() + 2000;

		while (!isInterrupted()) {
			long end = System.currentTimeMillis() + 1000;
			
			// when site timeout or relogin is true
			if (super.isLoginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.login(null);
			} else if(super.isReloginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.relogin(null);
			} else if (isMaxSiteTimeout()) {
				this.refreshEndTime();
				this.relogin(null);
			} else if (this.isSiteTimeout()) {
				// 发送心跳包到HOME节点，激活注册地址
				this.hello(local.getFamily(), getHubSite());
			}
			
			// 统计数据块标识号
			if (System.currentTimeMillis() >= time2) {
				checkChunkId();
				time2 += 2000;
			}

			long timeout = end - System.currentTimeMillis();
			if (timeout > 0) delay(timeout);
		}
		Logger.info("Launcher.process, site exit");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// 从HOME节点上注销
		logout(null);
		// 停止全部运行任务
		stopTask();
		// 停止JNI服务
		stopJNI();
		// 关闭池服务
		stopPool();
		// 停止FIXP监听
		stopListen();
		// 停止日志服务
		stopLog();
	}

	/*
	 * BuildTaskPool通知，命名池记录已经更新
	 * @see com.lexst.algorithm.TaskEventListener#updateNaming()
	 */
	@Override
	public void updateNaming() {
		Set<Naming> set = BuildTaskPool.getInstance().getNamings();

		// 本地更新，重新注册到HOME节点
		local.clear();
		for(Naming naming : set) {
			Project project = BuildTaskPool.getInstance().findProject(naming);
			for(Space space : project.getSpaces()) {
				local.add(naming, space);
			}
		}
		
		setOperate(BasicLauncher.RELOGIN);
	}

	/**
	 * check chunk identity
	 */
	private void checkChunkId() {
		int size = Install.countFreeChunkIds();
		if (size >= 5) return;
		// when missing, download from home site
		long[] allkey = applyChunkIds(50);
		// save to jni
		for (int i = 0; allkey != null && i < allkey.length; i++) {
			Install.addChunkId(allkey[i]);
		}
	}

	private long[] applyChunkIds(int num) {
		long[] chunkIds = puddle.poll(num);
		if (chunkIds != null && chunkIds.length >= num) {
			return chunkIds;
		}
		
		int left = (num - (chunkIds == null ? 0 : chunkIds.length));
		HomeClient client = bring();
		if (client == null) return chunkIds;
		ArrayList<Long> array = new ArrayList<Long>();
		
		for (int i = 0; chunkIds != null && i < chunkIds.length; i++) {
			array.add(chunkIds[i]);
		}
		try {
			long[] allkey = client.pullSingle(left);
			for (int i = 0; allkey != null && i < allkey.length; i++) {
				array.add(allkey[i]);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		// exit and close
		this.complete(client);
		
		// sort
		java.util.Collections.sort(array);
		chunkIds = new long[array.size()];
		for (int i = 0; i < array.size(); i++) {
			chunkIds[i] = array.get(i);
		}
		return chunkIds;
	}
	
	/**
	 * 上传数据块到其它节点
	 * @param request
	 * @param resp
	 * @return
	 * @throws IOException
	 */
	public boolean upload(Stream request, OutputStream resp) throws IOException {
		// chunk identity
		Message msg = request.findMessage(Key.CHUNK_ID);
		if (msg == null) return false;
		long chunkId = msg.longValue();
		// chunk resume breakpoint
		long breakpoint = 0L;
		msg = request.findMessage(Key.CHUNK_BREAKPOINT);
		if(msg != null) breakpoint = msg.longValue();
		// space
		msg = request.findMessage(Key.SCHEMA);
		if (msg == null) return false;
		String db = msg.stringValue();
		msg = request.findMessage(Key.TABLE);
		if (msg == null) return false;
		String table = msg.stringValue();
		Space space = new Space(db, table);
		Logger.info("Launcher.upload, space:'%s' chunkid:%x  breakpoint:%d", space, chunkId, breakpoint);

		// find chnunk and send to remote site
		BuildUploader uploader = new BuildUploader();
		byte[] b = Install.findChunkPath(db.getBytes(), table.getBytes(), chunkId);
		// when error, send a null
		if (b == null || b.length == 0) {
			Logger.error("Launcher.upload, cannot find '%s' - %x path", space, chunkId);
			uploader.execute(space, chunkId, breakpoint, null, resp);
			return false;
		}
		String filename = new String(b);
		boolean success = uploader.execute(space, chunkId, breakpoint, filename, resp);
		Logger.note(success, "Launcher.upload, send '%s' to %s", filename, request.getRemote());
		return success;
	}
	
	/**
	 * 从磁盘读取未使用的数据块标识号，保存到JNI接口
	 * @return
	 */
	private boolean loadIdentity() {
		// 加载并且解析未使用的数据块标识号
		File file = buildResourceFile(IdentityPuddle.filename);
		if(!file.exists()) return true;
		byte[] data = readFile(file);
		if(data == null) return false;
		puddle.parseXML(data);
		// 输出到JNI接口
		long[] chunkIds = puddle.pollAll();
		for (int i = 0; chunkIds != null && i < chunkIds.length; i++) {
			Install.addChunkId(chunkIds[i]);
		}
		return true;
	}
	
	/**
	 * 从JNI接口取回未使用的数据块标识号，写入磁盘保存!
	 * @return
	 */
	private boolean flushIdentity() {
		// 从JNI取回未用标识号
		long[] chunkIds = Install.getFreeChunkIds();
		Logger.info("Launcher.flushIdentity, chunk identity count %d", (chunkIds == null ? -1 : chunkIds.length));
		for (int i = 0; chunkIds != null && i < chunkIds.length; i++) {
			puddle.add(chunkIds[i]);
		}
		byte[] data = puddle.buildXML();
		// 写入磁盘
		File file = buildResourceFile(IdentityPuddle.filename);
		return flushFile(file, data);
	}
	
	/**
	 * 解析并且设置数据块存储目录
	 * @param document
	 * @return
	 */
	private boolean loadDirectory(Document document) {
		XMLocal xml = new XMLocal();
		Element element = (Element) document.getElementsByTagName("chunk-directory").item(0);
		String build = xml.getValue(element, "build");
		String cache = xml.getValue(element, "cache");
		String[] paths = xml.getXMLValues(element.getElementsByTagName("store"));
		// 设置重构存储目录
		int ret = Install.setBuildRoot(build.getBytes());
		Logger.note(ret == 0, "Launcher.loadDirectory, load build path %s", build);
		if (ret != 0) return false;
		// 设置存冲区目录
		ret = Install.setCacheRoot(cache.getBytes());
		Logger.note(ret == 0, "Launcher.loadDirectory, load cache path %s", cache);
		// 设置数据块目标目录
		if (ret != 0) return false;
		for (String path : paths) {
			ret = Install.setChunkRoot(path.getBytes());
			Logger.note(ret == 0, "Launcher.loadDirectory, load store path %s", path);
			if (ret != 0) return false;
		}
		return true;
	}

	/**
	 * 加载并且解析本地配置
	 * @param filename
	 * @return
	 */
	private boolean loadLocal(String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) {
			return false;
		}
		// 解析并且保存HOME节点地址
		SiteHost host = splitHome(document);
		if(host == null) {
			Logger.error("Launcher.loadLocal, cannot find home site");
			return false;
		}
		super.setHubSite(host);
		// 解析并且保存本地地址
		host = splitLocal(document);
		if(host == null) {
			Logger.error("Launcher.loadLocal, cannot find local site");
			return false;
		}
		local.setHost(host);
		
		// 解析远程关闭地址
		if (!loadShutdown(document)) {
			Logger.error("Launcher.loadLocal, cannot resolve shutdown");
			return false;
		}

		// 解析并且加载FIXP安全配置
		if (!super.loadSecurity(document)) {
			Logger.error("Launcher.loadLocal, cannot parse security file");
			return false;
		}

		// build节点资源配置目录
		String path = xml.getXMLValue(document.getElementsByTagName("resource-directory"));
		if (!createResourcePath(path)) {
			Logger.error("Launcher.loadLocal, cannot create directory %s", path);
			return false;
		}
		
		// build执发布管理目录
		path = xml.getXMLValue(document.getElementsByTagName("build-root"));
		Logger.info("Launcher.loadLocal, build deploy directory %s", path);
		if (path != null && path.length() > 0) {
			BuildTaskPool.getInstance().setRoot(path);
		}
		
		// 解析并且设置数据块存储目录
		if(!this.loadDirectory(document)) {
			Logger.error("Launcher.loadLocal, cannot resolve chunk directory!");
			return false;
		}
		
		return Logger.loadXML(filename);
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
		// 初始化JNI数据接口
		int ret = Install.initialize();
		if(ret != 0) {
			Logger.error("initialize failed, program will exit!");
			Logger.gushing();
			return;
		}
		// 加载并且解析本地配置
		String filename = args[0];
		boolean success = Launcher.getInstance().loadLocal(filename);
		Logger.note("Launcher.main, load local", success);
		// 加载数据块标识号
		if(success) {
			success = Launcher.getInstance().loadIdentity();
			Logger.note("Launcher.main, load identity", success);
		}
		// 启动进行
		if (success) {
			success = Launcher.getInstance().start();
			Logger.note("Launcher.main, start service", success);
		}
		if(!success) {
			Logger.gushing();
		}
	}
}