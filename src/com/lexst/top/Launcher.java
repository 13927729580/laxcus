/**
 *
 */
package com.lexst.top;

import java.io.*;

import org.w3c.dom.*;

import java.util.*;

import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.remote.client.home.*;
import com.lexst.remote.client.top.*;
import com.lexst.site.*;
import com.lexst.sql.account.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.schema.*;
import com.lexst.thread.*;
import com.lexst.top.effect.*;
import com.lexst.top.pool.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.visit.impl.top.*;
import com.lexst.xml.*;

public class Launcher extends HubLauncher {

	private static Launcher selfHandle = new Launcher();

	/** 当前节点绑定地址 **/
	private SiteHost local = new SiteHost();

	/** 数据库字典 **/
	private Dict dict = new Dict();

	/** 数据块标识生成器 **/
	private Single single = new Single();

	// table identity extractor
	private KeyManager keys = new KeyManager();

	/** 用户账号管理器 **/
	private UserManager manager = new UserManager();

	/** run-site handle */
	private TopExpressClient tracker;
	
	/** TOP运行节点。当前节点是后备节点时，此参数定义 **/
	private SiteHost runsite;
	
	/** 注册的HOME节点集合  **/
	private ArrayList<SiteHost> allsite = new ArrayList<SiteHost>();
	
	/**
	 * default
	 */
	private Launcher() {
		super();
		super.setExitVM(true);
		super.setLogging(true);
		packetImpl = new TopPacketInvoker(fixpPacket);
		streamImpl = new TopStreamInvoker();
	}

	/**
	 * get instance
	 * @return
	 */
	public static Launcher getInstance() {
		return Launcher.selfHandle;
	}
	
	public SiteHost getLocalHost() {
		return this.local;
	}
	
	public UserManager getUserManager() {
		return this.manager;
	}

	public Dict getDict() {
		return this.dict;
	}
	
	public Single getSingle() {
		return this.single;
	}
	
	public KeyManager getSpaceKey() {
		return this.keys;
	}

	public Schema showSchema(String schema) {
		return dict.findSchema(schema);
	}

	public Table showTable(String schema, String table) {
		Space space = new Space(schema, table);
		return dict.findTable(space);
	}
	
	/**
	 * fixp rpc 调用
	 */
	public void nothing() {
		// rpc call
	}
	
	/**
	 * 显示总集群中某一类节点地址
	 * 
	 * @param siteFamily - 节点ID号，0代表所有节点
	 * @param from
	 * @return
	 */
	public SiteHost[] showSite(int siteFamily, Address[] from) {
		List<SiteHost> homes = HomePool.getInstance().gather();

		ArrayList<SiteHost> array = new ArrayList<SiteHost>();
		// 如果显示HOME节点地址时，不考虑froms单元参数
		if (siteFamily == 0 || siteFamily == Site.HOME_SITE) {
			array.addAll(homes);
		}
		
		if (siteFamily == 0 || siteFamily != Site.HOME_SITE) {
			SiteHost host = null;
			if (from != null) {
				for (SiteHost home : homes) {
					// 地址不匹配就继续下一个，否则保留
					if(!home.getAddress().matchsIn(from)) continue;
					host = home;
					break;
				}
			} else {
				host = HomePool.getInstance().findRunsite();
			}
			if (host == null) return null;
			
			// 启动进行节点检索
			HomeClient client = new HomeClient(host.getPacketHost());
			try {
				client.reconnect();
				SiteHost[] hosts = client.showSite(siteFamily);
				for (int i = 0; hosts != null && i < hosts.length; i++) {
					array.add(hosts[i]);
				}
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
		}
		
//		Logger.debug("Launcher.showSite, site size:%d", array.size());
		
		if(array.isEmpty()) return null;
		SiteHost[] hosts = new SiteHost[array.size()];
		return array.toArray(hosts);
	}

	/**
	 * return a home client handle
	 * @return
	 */
	private HomeClient bring(SiteHost home) {
		SocketHost address = home.getStreamHost();
		HomeClient client = new HomeClient(true, address);
		for (int i = 0; i < 3; i++) {
			try {
				client.reconnect();
				return client;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(1000);
		}
		return null;
	}

	/**
	 * @param remote
	 * @return
	 */
	private TopExpressClient fetch(SocketHost remote) {
		TopExpressClient client = new TopExpressClient(remote);
		for (int i = 0; i < 3; i++) {
			try {
				client.reconnect();
				return client;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(1000);
		}
		return null;
	}
	
	/**
	 * @param client
	 */
	private void complete(HomeClient client) {
		if(client == null) return;
		try {
			client.exit();
			client.close();
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
	}
	
	/**
	 * close client
	 * @param client
	 */
	private void complete(TopExpressClient client) {
		if (client == null) return;
		try {
			client.exit();
			client.close();
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
	}

	/**
	 * implement close
	 * @param client
	 */
	private void closeup(TopExpressClient client) {
		if (client != null) {
			client.close();
		}
	}
	
	
	/**
	 * start manager pool
	 * @return
	 */
	private boolean loadPool() {
		Logger.info("Launcher.loadPool, start task pool...");
		//set packet listener
		HomePool.getInstance().setPacketListener(fixpPacket);
		LivePool.getInstance().setPacketListener(fixpPacket);
		
		// load home pool
		boolean success = HomePool.getInstance().start();
		// load sql pool
		if(success) {
			this.delay(500);
			success = LivePool.getInstance().start();
			if(!success) {
				HomePool.getInstance().stop();
			}
		}
		
		if(success) {
			while(!HomePool.getInstance().isRunning()) {
				this.delay(200);
			}
			while(!LivePool.getInstance().isRunning()) {
				this.delay(200);
			}
		}
		
		return success;
	}

	/**
	 * stop manager pool
	 */
	private void stopPool() {
		Logger.info("Launcher.stopPool, stop task pool...");

		HomePool.getInstance().stop();
		LivePool.getInstance().stop();
		
		while (HomePool.getInstance().isRunning()) {
			this.delay(500);
		}
		while(LivePool.getInstance().isRunning()) {
			this.delay(500);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// stop listener
		stopListen();
		// stop task pool
		stopPool();
		// stop log service
		stopLog();
		// save configure
		flushResouce();
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// 1. load log service
		boolean success = Logger.loadService(null);
		if(!success) {
			Logger.error("Launcher.init, cannot load log service");
			return false;
		}
		// 2. load top service
		if (success) {			
			// start listen
			Class<?>[] clses = { TopVisitImpl.class, TopExpressImpl.class };
			success = loadListen(clses, local);
			Logger.note("Launcher.init, load listen", success);
			if (!success) {
				stopLog();
			}
		}
		// 3. load manager pool (home and sqlive manager)
		if (success) {
			success = loadPool();
			Logger.note("Launcher.init, load pool", success);
			if (!success) {
				stopListen();
				stopLog();
			}
		}
		// 4. choose run site or backup site
		if (success) {
			boolean runstat = this.discuss(local, super.backups);
			Logger.info("Launcher.init, current mode is '%s'", (runstat ? "run site" : "backup site"));
			if (runstat) {

			} else {

			}
		}
		
		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("Launcher.process, into...");

		while (!super.isInterrupted()) {
			if (isRunsite()) {
				this.watch();
			} else {
				stakeout();
			}
		}

		Logger.info("Launcher.process, exit");
	}
	
	/**
	 * in run-site status
	 */
	private void watch() {
		Logger.info("Launcher.watch, into...");
		
		while (!isInterrupted()) {
			this.check();
			this.delay(5000);
		}
	}

	/**
	 * in not run-site status
	 */
	private void stakeout() {
		Logger.info("Launcher.stakeout, into...");
		
		long copytime = super.getCopyInterval() * 1000;
		long activetime = super.getActiveInterval() * 1000;
		long endcopy = System.currentTimeMillis() + copytime;
		long endactive = System.currentTimeMillis() + activetime;
		
		while (!isInterrupted() && !isRunsite()) {
			// sleep
			this.delay(1000);
			// connect run-site
			if (!this.smell()) {
				delay(9000);
				continue;
			}
			
			if (System.currentTimeMillis() >= endcopy) {
				endcopy += copytime;
				if(!this.backup()) continue;
			}
			if (System.currentTimeMillis() >= endactive) {
				endactive += activetime;
				this.active();
			}
		}
		
		if(tracker != null) {
			this.complete(tracker);
			tracker = null;
		}
	}
	
	/**
	 * on connect status, return true
	 * connect success, return true
	 * otherwise false
	 * @return
	 */
	private boolean smell() {
		if (tracker != null) {
			return true;
		}
		
		SiteHost host = findRunsite();
		if (host == null) {
			Logger.warning("Launcher.smell, cannot find top-site");
			return false;
		}

		// connect run-site (keep udp)
		boolean success = false;
		try {
			tracker = new TopExpressClient(host.getPacketHost());
			tracker.setRecvTimeout(30);
			tracker.reconnect();
			tracker.nothing();
			runsite = new SiteHost(host);
			success = true;
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (!success) {
			this.closeup(tracker);
			tracker = null;
		}
		return success;
	}

	/**
	 * find run-site
	 * @return
	 */
	private SiteHost findRunsite() {
		for (SiteHost host : super.backups) {
			TopExpressClient client = fetch(host.getStreamHost());
			if (client == null) continue;
			try {
				boolean flag = client.isRunsite();
				if (flag) {
					return new SiteHost(host);
				}
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			} finally {
				this.complete(client);
			}
		}
		return null;
	}

	/**
	 * active operate
	 * @return
	 */
	private boolean active() {
		if (tracker == null) {
			Logger.warning("Launcher.active, invalid handle!");
			return false;
		}

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			if (i > 0) this.delay(2000);
			try {
				if(tracker.isClosed()) tracker.reconnect();
				tracker.nothing();
				success = true; 
				break;
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			tracker.close();
		}
		
		if(!success) {
			this.closeup(tracker);
			tracker = null;
			
			// 重新尝试连接不成功,去询问HOME站点
			if(!this.smell()) {
				this.verdict();
			}
		}

		return success;
	}
	
	/**
	 * check run-site status
	 * 
	 * @return
	 */
	private int evaluate() {
		long check_time = super.getDetectInterval() * 1000; // milli-second
		// query home site, check run-topsite
		JobCrawler crawler = new JobCrawler();
		int status = crawler.detect(allsite, runsite, check_time);
		return status;
	}

	/**
	 * notify all site (home site), re-login to self
	 * 
	 * @param hosts
	 */
	protected void transfer(List<SiteHost> hosts) {
		Command cmd = new Command(Request.NOTIFY, Request.TRANSFER_HUB);
		Packet packet = new Packet(cmd);
		packet.addMessage(Key.LOCAL_ADDRESS, local.toString());

		for (SiteHost host : hosts) {
			SocketHost remote = host.getPacketHost();
			for (int i = 0; i < 3; i++) {
				fixpPacket.send(remote, packet);
			}
		}
	}
	
	/**
	 * query run-site
	 */
	private void verdict() {		
		while (!super.isInterrupted()) {
			// 询问全部home-site,指定的地址是否已经失效
			int status = this.evaluate();
			Logger.info("Launcher.verdict, evaluate result: %s", JobCrawler.explain(status));

			if (status == JobCrawler.UNDEFINE) {
				this.delay(5000);
			} else if (status == JobCrawler.EXISTED) {
				break;
			} else if (status == JobCrawler.NOTFOUND) {
				// choose a new run-site
				boolean runstat = this.discuss(local, super.backups);
				Logger.info("Launcher.verdict, current mode is '%s'", (runstat ? "run site" : "backup site"));
				// when run-status, relogin to self 
				if (runstat) {
					// notify all home-site, relogin here
					this.transfer(this.allsite);
				}
				break;
			}
		}
	}
	
	/**
	 * copy resource data from run-site
	 * @return
	 */
	private boolean backup() {
		if (tracker == null) {
			return false;
		}

		boolean success = false;
		try {
			byte[] data = tracker.dict();
			dict.clear();
			this.dict.parseXML(data);

			long value = tracker.single();
			this.single.setBegin(value);

			data = tracker.pid();
			this.keys.clear();
			keys.parseXML(data);

			data = tracker.accounts();
			manager.clear();
			manager.parseXML(data);
			
			SiteHost[] hosts = tracker.findSite(Site.HOME_SITE);
			allsite.clear();
			saveto(hosts, allsite);
			
			success = true;
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}

		if (success) {
			flushResouce();
		} else {
			closeup(tracker);
			tracker = null;

			// home-site scan top site
			if (!this.smell()) {
				this.verdict();
			}
		}
		return success;
	}
	
	/**
	 * save host address
	 * @param hosts
	 * @param array
	 */
	private void saveto(SiteHost[] hosts, List<SiteHost> array) {
		for (int i = 0; hosts != null && i < hosts.length; i++) {
			array.add(hosts[i]);
		}
	}

	/**
	 * check jobs
	 */
	private void check() {
		// 检测数据表的重构触发时间，当达到指定时间后，向HOME节点发送消息
		for (String name : dict.keys()) {
			Schema schema = dict.findSchema(name);
			for (Space space : schema.spaces()) {
				SwitchTime switchTime = schema.findSwitchTime(space);
				if (switchTime == null ) continue;
				
				// 达到重构时间
				if (switchTime.isTouched()) {
					SiteHost[] hosts = HomePool.getInstance().find(space);
					// 向每个HOME节点发送消息，启动重构
					for (int i = 0; hosts != null && i < hosts.length; i++) {
						sendRebuild(hosts[i], space, switchTime.getColumnId());
					}
					// 计算下一次触发时间
					switchTime.nextTouch();
				}
			}
		}
	}

	/**
	 * send rebuild command to home site
	 * @param host
	 * @param space
	 * @return
	 */
	private Address[] sendRebuild(SiteHost host, Space space, short columnId) {
		String db = space.getSchema();
		String table = space.getTable();
		boolean success = false;
		Address[] addresses = null;
		
		HomeClient client = this.bring(host);
		if(client != null) {
			try {
				addresses = client.rebuild(db, table, columnId, null);
				success = true;
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
		}
		complete(client);
		
		Logger.note(success, "Launcher.sendRebuild, send command to %s, accepted address size %d",
				host, (addresses == null ? 0 : addresses.length));
		if (success) {
			for (int i = 0; addresses != null && i < addresses.length; i++) {
				Logger.info("Launcher.sendRebuild, accepted host %s", addresses[i]);
			}
		}
		
		return addresses;
	}

	/**
	 * stop log serivce
	 */
	private void stopLog() {
		Logger.stopService();
	}
	
	/**
	 * apply a chunk number
	 * @param num
	 * @return
	 */
	public long[] pullSingle(int num) {
		return single.pull(num);
	}
	
	/**
	 * @param db
	 * @param table
	 * @return
	 */
	public Table findTable(String db, String table) {
		return findTable(new Space(db, table));
	}

	/**
	 * find a table configure
	 *
	 * @param db
	 * @param table
	 * @return
	 */
	public Table findTable(Space space) {
		Logger.info("Launcher.findTable, space is %s", space);
		return dict.findTable(space);
	}

	/**
	 * 根据注册用户名，检查注册账号是否存在
	 * @param user
	 * @return
	 */
	public boolean onUser(User user) {
		Logger.info("Launcher.onUser, username '%s'", user.getHexUsername());
		if (manager.isDBA(user)) {
			Logger.info("Launcher.onUser, this is dba '%s'", user.getHexUsername());
			return true;
		}
		return manager.findAccount(user) != null;
	}

	/**
	 * create a database
	 * @param schema
	 * @param pwd
	 * @return
	 */
	public boolean createSchema(Schema schema) {
		Logger.info("Launcher.createSchema, database name '%s'", schema.getName());
		boolean success = dict.addSchema(schema);
		if (success) {
			this.flushDict();
		}

		Logger.note(success, "Launcher.createSchema, create database '%s'", schema.getName());
		
		return success;
	}

	/**
	 * delete a database and all table of database
	 * @param db
	 * @param pwd
	 * @return
	 */
	public boolean deleteSchema(String db) {
		Logger.info("Launcher.deleteSchema, database name '%s'", db);
		Schema base = dict.deleteSchema(db);
		if (base == null) {
			Logger.error("cannot delete database '%s'", db);
			return false;
		}

		Logger.info("Launcher.deleteSchema, delete database '%s' success", db);

		// delete all table space
		for (Table table : base.listTable()) {
			Space space = table.getSpace();
			HomePool.getInstance().deleteSpace(space);
		}
		this.flushDict();
		return true;
	}

	/**
	 * create a database table
	 * @param pwd
	 * @param table
	 * @return
	 */
	public boolean createSpace(Table table) {
		Space space = table.getSpace();
		if (dict.exists(space)) {
			Logger.warning("Launcher.createSpace, table space '%s' existed", space);
			return false;
		}
		// 1. create table space to data site
		boolean success = HomePool.getInstance().createSpace(table);
		// 2. when error, exit
		if (!success) {
			Logger.error("Launcher.createSpace, cannot create table '%s'", space);
			return false;
		}
		//3. save table
		dict.addTable(space, table);
		//4. init pid
		ColumnAttribute field = table.pid();
		Number value = null;
		// 只允许这5种类型生成主键
		if (field.isShort()) value = new Short((short) 0);
		else if (field.isInteger()) value = new Integer(0);
		else if (field.isLong()) value = new Long(0);
		else if (field.isFloat()) value = new Float(0);
		else if (field.isDouble()) value = new java.lang.Double(0);
		if (value != null) {
			keys.set(space, value);
			this.flushKey();
		} else {
			Logger.warning("Launcher.createSpace, cannot create pid by '%s'", space);	
		}
		// flush to disk
		this.flushDict();
		Logger.note(success, "Launcher.createSpace, create table '%s'", space);
		return true;
	}

	/**
	 * remove table head
	 * @param space
	 * @return
	 */
	public boolean deleteSpace(Space space) {
		Logger.info("Launcher.deleteSpace, delete table space '%s'", space);
		
		// delete all table from home site
		boolean	success = HomePool.getInstance().deleteSpace(space);
		// delete disk
		if(success) {
			dict.deleteTable(space);
			this.flushDict();
		}
		Logger.note(success, "Launcher.deleteSapce, delete table space '%s'", space);
		return success;
	}
	
	/**
	 *
	 */
	private void flushResouce() {
		this.flushDict();
		this.flushAccount();
		this.flushSingle();
		this.flushKey();
	}



	/**
	 * 加载数据库管理员账号
	 * @param filename
	 * @return
	 */
	private boolean loadDBA(String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) {
			return false;
		}
		
		// 管理员账号的用户名和密码
		String username = xml.getXMLValue(document.getElementsByTagName("username"));
		String password = xml.getXMLValue(document.getElementsByTagName("password"));
		
		if (username.length() > 0 && password.length() > 0) {
			Administrator dba = manager.getDBA();
			dba.setHexUsername(username);
			dba.setHexPassword(password);
			return true;
		}

		return false;
	}

	/**
	 * @param filename
	 * @return
	 */
	private boolean loadLocal(String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) {
			Logger.error("Launcher.loadLocal, cannot parse %s", filename);
			return false;
		}
		
		// TOP节点本地绑定地址
		SiteHost host = super.splitLocal(document);
		if (host == null) {
			Logger.error("Launcher.loadLocal, cannot parse local address");
			return false;
		}
		local.set(host);
		Logger.info("Launcher.loadLocal, local address:%s", host);
		
		// 后备监视TOP节点地址
		if(!super.loadBackups(document)) {
			Logger.error("Launcher.loadLocal, cannot resolve backup address!");
			return false;
		}
		
		// TOP节点配置目录
		String s = xml.getXMLValue(document.getElementsByTagName("resource-directory"));
		if (!super.createResourcePath(s)) {
			Logger.error("Launcher.loadLocal, cannot create path %s", s);
			return false;
		}
		
		// 解析并设置FIXP监视器安全配置
		if(!super.loadSecurity(document)) {
			Logger.error("Launcher.loadLocal, cannot resolve safe file");
			return false;
		}
		
		// 数据库管理员账号目录
		s = xml.getXMLValue(document.getElementsByTagName("dba-account"));
		if (!loadDBA(s)) {
			Logger.error("cannot resolve dba configure");
			return false;
		}
		
		// 节点检查触发时间间隔
		s = xml.getXMLValue(document.getElementsByTagName("sleep-time"));
		int sleepTime = Integer.parseInt(s);
		HomePool.getInstance().setSleep(sleepTime);
		LivePool.getInstance().setSleep(sleepTime);
		// 注册节点超时时间(所有节点的超时时间是一致的)
		s = xml.getXMLValue(document.getElementsByTagName("site-timeout"));
		int siteTimeout = Integer.parseInt(s);
		super.setSiteTimeout(siteTimeout);
		HomePool.getInstance().setSiteTimeout(siteTimeout);
		LivePool.getInstance().setSiteTimeout(siteTimeout);
		// 节点发生超时到被删除之间的时间间隔(通常是超时时间的3倍)
		s = xml.getXMLValue(document.getElementsByTagName("delete-timeout"));
		int deleteTimeout = Integer.parseInt(s);
		HomePool.getInstance().setDeleteTimeout(deleteTimeout);
		LivePool.getInstance().setDeleteTimeout(deleteTimeout);

		// 加载并且解析日志配置
		return Logger.loadXML(filename);
	}

	/**
	 * 加载数据字典
	 * @return
	 */
	private boolean loadDict() {
		File file = buildResourceFile(Dict.filename);
		// not found, return true;
		if (!file.exists()) return true;
		byte[] b = readFile(file);
		return dict.parseXML(b);
	}

	private void flushDict() {
		byte[] b = dict.buildXML();
		File file = buildResourceFile(Dict.filename);
		this.flushFile(file, b);
	}

	/**
	 * 加载注册用户账号
	 * @return
	 */
	private boolean loadAccount() {
		File file = buildResourceFile(UserManager.filename);
		// not found, return true;
		if (!file.exists()) {
			return true;
		}
		byte[] b = readFile(file);
		return manager.parseXML(b);
	}

	public void flushAccount() {
		File file = buildResourceFile(UserManager.filename);
		byte[] b = manager.buildXML();
		this.flushFile(file, b);
	}

	private boolean loadSingle() {
		File file = buildResourceFile(Single.filename);
		// not found, return true;
		if(!file.exists()) return true;
		byte[] b = readFile(file);
		return single.parseXML(b);
	}

	private void flushSingle() {
		byte[] b = single.buildXML();
		File file = buildResourceFile(Single.filename);
		flushFile(file, b);
	}
	
	private boolean loadKey() {
		File file = buildResourceFile(KeyManager.filename);
		if(!file.exists()) return true;
		byte[] b = readFile(file);
		return keys.parseXML(b);
	}
	
	/**
	 * flush pid
	 * @param space
	 * @param num
	 * @return
	 */
	public Number[] pullKey(String schema, String table, int num) {
		if (schema == null || table == null) {
			return null;
		}
		Space space = new Space(schema, table);
		return keys.pull(space, num);
	}
	
	/**
	 * @param schema
	 * @param table
	 * @return
	 */
	public int findChunkSize(String schema, String table) {
		if (schema == null || table == null) {
			return -1;
		}
		Space space = new Space(schema, table);
		return dict.findChunkSize(space);
	}

	/**
	 * set chunk size
	 * @param space
	 * @param size
	 * @return
	 */
	public boolean setChunkSize(Space space, int size) {
		Logger.debug("Launcher.setChunkSize, chunk %s size %d", space, size);
		boolean success = dict.setChunkSize(space, size);
		if(success) flushDict();
		return success;
	}

	/**
	 * 设置表的数据重构时间
	 * @param space
	 * @param columnId
	 * @param type
	 * @param time
	 * @return
	 */
	public boolean setRebuildTime(Space space, short columnId, int type, long time) {
		Logger.debug("Launcher.setRebuildTime, time %s - %d - %d - %d",
				space, columnId, type, time);
		
		boolean success = dict.setRebuildTime(space, columnId, type, time);
		if (success) flushDict();
		return success;
	}

	/**
	 * 协商,如果是local,返回true. 否则返回false
	 * 
	 * @param local
	 * @param friends
	 * @return
	 */
	private boolean discuss(SiteHost local, List<SiteHost> friends) {
		List<SiteHost> a = new ArrayList<SiteHost>(friends);
		a.add(local);
		SiteHost[] hosts = new SiteHost[a.size()];
		a.toArray(hosts);

		List<SiteHost> records = new ArrayList<SiteHost>();
		int nulls = 0;
		for(SiteHost host : friends) {
			TopExpressClient client = fetch(host.getStreamHost());
			if(client == null) {
				nulls++;
				continue; //connect failed! next host
			}
			try {
				SiteHost address = client.voting(hosts);
				if(address != null) records.add(address);
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			this.complete(client);
		}

		if (nulls == friends.size() || records.size() == friends.size()) {
			for (SiteHost host : records) {
				if (!host.equals(local)) return false;
			}
			runsite = null;
			super.setRunsite(true);
			return true;
//			return (this.runflag = true);
		}
		return false;
	}

	/**
	 * save table pid
	 */
	private void flushKey() {
		byte[] data = keys.buildXML();
		File file = buildResourceFile(KeyManager.filename);
		this.flushFile(file, data);
	}

	private boolean loadResource() {
		boolean success = this.loadDict();
		Logger.note("Launcher.loadResource, load dict", success);
		if(success) {
			success = this.loadAccount();
		}
		Logger.note("Launcher.loadResource, load account", success);
		if(success) {
			success = this.loadSingle();
		}
		Logger.note("Launcher.loadResource, load machine", success);
		if(success) {
			success = this.loadKey();
		}
		Logger.note("Launcher.loadResource, load PID", success);
		return success;
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
		
		String filename = args[0];
		boolean success = Launcher.getInstance().loadLocal(filename);
		Logger.note("Launcher.main, load local", success);
		if (success) {
			success = Launcher.getInstance().loadResource();
			Logger.note("Launcher.main, load resource", success);
		}
		if (success) {
			success = Launcher.getInstance().start();
			Logger.note("Launcher.main, start service", success);
		}
		
		if (!success) {
			Logger.gushing();
		}
	}

}