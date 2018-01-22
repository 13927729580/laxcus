/**
 *
 */
package com.lexst.home;

import java.io.*;
import java.net.*;
import java.util.*;

import org.w3c.dom.*;

import com.lexst.fixp.*;
import com.lexst.home.effect.*;
import com.lexst.home.pool.*;
import com.lexst.log.client.*;
import com.lexst.remote.client.top.*;
import com.lexst.site.*;
import com.lexst.site.home.*;
import com.lexst.sql.schema.*;
import com.lexst.thread.*;
import com.lexst.util.datetime.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.visit.impl.home.*;
import com.lexst.xml.*;
import com.lexst.remote.client.home.HomeExpressClient;

public class Launcher extends HubLauncher {

	private static Launcher selfHandle = new Launcher();

	/** TOP节点地址 **/
	private SiteHost top = new SiteHost();

	/** 本地节点绑定地址 **/
	private HomeSite local = new HomeSite();
	
	// chunk identity array
	private IdentitySet idset = new IdentitySet();
	// user space set
	private UserSpace spaces = new UserSpace();
		
	/* run-site handle */
	private HomeExpressClient tracker;
	private SiteHost runsite;
	/* all home site */
	private ArrayList<SiteHost> allsite = new ArrayList<SiteHost>();

	/**
	 * private constructor
	 */
	private Launcher() {
		super();
		super.setExitVM(true);
		super.setLogging(true);
		packetImpl = new HomePacketInvoker(fixpPacket);
		streamImpl = new HomeStreamInvoker();
	}

	/**
	 * return a static handle
	 * @return
	 */
	public static Launcher getInstance() {
		return Launcher.selfHandle;
	}
	
	public void setHub(SiteHost host) {
		this.top.set(host);
	}
	
	/**
	 * return local listen address
	 * @return
	 */
	public SiteHost getLocalHost() {
		return local.getHost();
	}

	/**
	 * local host site
	 * @return
	 */
	public HomeSite getLocal() {
		return this.local;
	}
	
	public IdentitySet getIdentitySet() {
		return this.idset;
	}

	public UserSpace getUserSpace() {
		return this.spaces;
	}
	
	/**
	 * 以平均分配的方式分配一部分数据库表给调用方，以求达到在环境中平衡数据计算的目标
	 * @param apply
	 * @return
	 */
	public Space[] balance(int apply) {
		Map<Space, Integer> datas = DataPool.getInstance().measure();
		Map<Space, Integer> calls = CallPool.getInstance().measure();
		Map<Integer, List<Space>> group = new TreeMap<Integer, List<Space>>();
		
		// 找到分配数量最小的数据库表
		for(Space space : datas.keySet()) {
			int num1 = datas.get(space);
			int left = 0;
			if(calls.containsKey(space)) {
				int num2 = calls.get(space);
				left = num2 - num1;
			} else {
				left = 0 - num1;
			}
			List<Space> list = group.get(left);
			if(list == null) {
				list = new ArrayList<Space>();
				group.put(left, list);
			}
			list.add(space);
		}

		ArrayList<Space> array = new ArrayList<Space>();
		for (int left : group.keySet()) {
			List<Space> list = group.get(left);
			if (left < 0) {
				array.addAll(list);
			} else {
				java.util.Collections.sort(list);
				for(Space space : list) {
					if (array.size() >= apply) break;
					array.add(space);
				}
			}
		}
		
		if (array.isEmpty()) return null;
		Space[] s = new Space[array.size()];
		return array.toArray(s);
	}
	
	/**
	 * @param db
	 * @param table
	 * @return
	 */
	public int findChunkSize(String db, String table) {
		Space space = new Space(db, table);
		int size = spaces.findChunkSize(space);
				
		Logger.debug("Launcher.findChunkSize, find '%s' chunk size %d", space, size);

		return size;
	}
	
	/**
	 * query task naming (all,diffuse,aggregate,build) and ip address
	 * @param tag
	 * @param sites
	 * @return
	 */
	public String showTask(String tag, Address[] sites) {
		StringBuilder buff = new StringBuilder();
		if ("diffuse".equalsIgnoreCase(tag) || "all".equalsIgnoreCase(tag)) {
			String s = DataPool.getInstance().showTask(sites);
			buff.append(s);
		}
		if ("aggregate".equalsIgnoreCase(tag) || "all".equalsIgnoreCase(tag)) {
			String s = WorkPool.getInstance().showTask(sites);
			buff.append(s);
		}
		if ("build".equalsIgnoreCase(tag) || "all".equalsIgnoreCase(tag)) {
			String s = BuildPool.getInstance().showTask(sites);
			buff.append(s);
		}
		return buff.toString();
	}
	
	/**
	 * fixp rpc calling
	 */
	public void nothing() {
		// rpc call
	}

	/**
	 * apply a chunk number
	 * @param num
	 * @return
	 */
	public long[] pullSingle(int num) {
		if(idset.isEmpty()) {
			this.loadChunkIds(null);
		}
		if(idset.isEmpty()) return null;
		return idset.apply(num);
	}

	/**
	 * apply table pid
	 * @param db
	 * @param table
	 * @param num
	 * @return
	 */
	public Number[] pullKey(String db, String table, int num) {
		TopClient client = fetch(top);
		if (client == null) return null;
		Number[] nums = null;
		try {
			nums = client.pullKey(db, table, num);
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			complete(client);
		}
		
		Logger.debug("Apply pid by %s:%s size %d", db, table, (nums == null ? -1 : nums.length));
		if(nums != null) {
			Logger.debug("Launcher.pullKey, class name is %s", nums[0].getClass().getName());
		}
		
		return nums;
	}

	/**
	 * find a table configure from memory
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
		Logger.info("Launcher.findTable, space is '%s'", space);
		return spaces.find(space);
	}

	/**
	 * create a database table
	 * @param pwd
	 * @param table
	 * @return
	 */
	public boolean createSpace(Table table) {
		Space space = table.getSpace();
		if(spaces.exists(space)) {
			Logger.warning("Launcher.createSpace, table space '%s' existed!", space);
			return false;
		}
		int sites = CallPool.getInstance().countSite();
		if (sites == 0) {
			Logger.error("Launcher.createSpace, call site missing");
			return false;
		}
		sites = DataPool.getInstance().countSite();
		if (sites == 0) {
			Logger.error("Launcher.createSpace, data site missing");
			return false;
		}

		Logger.info("Launcher.createSpace, create table space '%s'", space);
		//1. create table space to data site
		SiteHost[] hosts = DataPool.getInstance().createSpace(table);
		boolean success = (hosts != null);
		//2. when success, create table space to call site
		if (success) {
			success = CallPool.getInstance().createSpace(table);
			// when failed, delete space from date site
			if(!success) {
				for(SiteHost host : hosts) {
					DataPool.getInstance().deleteSpace(host, space);
				}
			}
		}
		//3. when success, wirte local dict
		if (success) {
			// update table and write disk
			spaces.add(space, table);
			local.addSpace(space);
			
			flushSpace();
			local.setPublish(true);
		}
		
		Logger.note(success, "Launcher.createSpace, create table '%s'", space);
		return success;
	}

	/**
	 * remove table space
	 * @param db
	 * @param table
	 * @return
	 */
	public boolean deleteSpace(String db, String table) {
		return deleteSpace(new Space(db, table));
	}

	/**
	 * remove table head
	 * @param space
	 * @return
	 */
	public boolean deleteSpace(Space space) {
		Logger.info("Launcher.deleteSpace, delete table space '%s'", space);
		// delete all table space from data site
		boolean success = DataPool.getInstance().deleteSpace(space);
		// when success, delete table space from call site
		if (success) {
			success = CallPool.getInstance().deleteSpace(space);
		}
		if(success) {
			spaces.remove(space);
			local.removeSpace(space);
			
			flushSpace();
			local.setPublish(true);
		}
		Logger.note(success, "Launcher.deleteSpace, delete table space '%s'", space);
		return success;
	}

	/**
	 * start manager pool
	 * @return
	 */
	private boolean loadPool() {
		Logger.info("Launcher.loadPool, load all pool...");
		//set packet listener
		WorkPool.getInstance().setPacketListener(fixpPacket);
		BuildPool.getInstance().setPacketListener(fixpPacket);
		CallPool.getInstance().setPacketListener(fixpPacket);
		DataPool.getInstance().setPacketListener(fixpPacket);
		LogPool.getInstance().setPacketListener(fixpPacket);

		// load manage pool
		boolean success = LogPool.getInstance().start();
		if (success) {
			this.delay(500);
			success = DataPool.getInstance().start();
		}
		if (success) {
			this.delay(500);
			success = CodePointPool.getInstance().start();
		}
		if (success) {
			this.delay(500);
			success = CallPool.getInstance().start();
		}
		if(success) {
			this.delay(500);
			success = BuildPool.getInstance().start();
		}
		if(success) {
			this.delay(500);
			success = WorkPool.getInstance().start();
		}
		
		if(success) {
			while(!LogPool.getInstance().isRunning()) {
				this.delay(200);
			}
			while(!DataPool.getInstance().isRunning()) {
				this.delay(200);
			}
			while(!CallPool.getInstance().isRunning()) {
				this.delay(200);
			}
			while(!BuildPool.getInstance().isRunning()) {
				this.delay(200);
			}
			while(!WorkPool.getInstance().isRunning()) {
				this.delay(200);
			}
		} else {
			stopPool();
		}
		
		return success;
	}

	/**
	 * stop manager pool
	 */
	private void stopPool() {
		Logger.info("Launcher.stopPool, stop all pool...");

		BuildPool.getInstance().stop();
		WorkPool.getInstance().stop();
		CallPool.getInstance().stop();
		DataPool.getInstance().stop();
		CodePointPool.getInstance().stop();
		LogPool.getInstance().stop();

		while (BuildPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (WorkPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (CallPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (DataPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (CodePointPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (LogPool.getInstance().isRunning()) {
			this.delay(200);
		}
	}

	/**
	 * connect host site and return handle
	 * @return
	 */
	private TopClient fetch(SiteHost host) {
		TopClient client = new TopClient(true, host.getStreamHost());
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
			delay(1000);
		}
		return null;
	}

	/**
	 * @param client
	 */
	private void complete(TopClient client) {
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
	 * @param remote
	 * @return
	 */
	private HomeExpressClient fetch(SocketHost remote) {
		HomeExpressClient client = new HomeExpressClient(remote);
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
	 * close client
	 * @param client
	 */
	private void complete(HomeExpressClient client) {
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
	private void closeup(HomeExpressClient client) {
		if (client != null) {
			client.close();
		}
	}

	/**
	 * set top time
	 * @param client
	 * @return
	 */
	private boolean loadTime(TopClient client) {
		boolean nullable = (client == null);
		if (nullable) client = fetch(top);
		if (client == null) return false;

		boolean success = false;
		for(int i = 0; i < 3; i++) {
			try {
				long time = client.currentTime();
				Logger.info("Launcher.loadTime, set time:%d", time);
				if (time != 0L) {
					int ret = SystemTime.set(time);
					success = (ret == 0);
				}
				break;
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			delay(1000);
		}
		if (nullable) complete(client);
		
		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
		String time = sdf.format(new java.util.Date(System.currentTimeMillis()));
		Logger.note(success, "Launcher.loadTime, current time:%s", time);
		
		return success;
	}

	/**
	 * login to top site
	 * @param client
	 * @return
	 */
	private boolean login(TopClient client) {
		Logger.info("Launcher.login, %s login to %s", local.getHost(), top);
		boolean nullable = (client == null);
		if (nullable) client = fetch(top);
		if (client == null) return false;
		
		local.setRunsite( super.isRunsite() );
		
		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.login(local);
				break;
			} catch (VisitException exp) {
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
	 * relogn to top site
	 * @param site
	 * @return
	 */
	private boolean relogin(TopClient client) {
		Logger.info("Launcher.relogin, %s relogin to %s", local.getHost(), top);
		boolean nullable = (client == null);
		if (nullable) client = fetch(top);
		if (client == null) return false;
		
		local.setRunsite(isRunsite());
		
		boolean success = false;
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
		if(nullable) complete(client); 
		return success;
	}

	/**
	 * logout from top site
	 * @param client
	 * @return
	 */
	private boolean logout(TopClient client) {
		Logger.info("Lancher.logout, %s from %s", local.getHost(), top);
		boolean nullable = (client == null);
		if(nullable) client = fetch(top);
		if(client == null) return false;
		
		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.logout(local.getFamily(), local.getHost());
				break;
			} catch (VisitException exp) {
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
	 * apply chunk identity set
	 * @param client
	 * @return
	 */
	private boolean loadChunkIds(TopClient client) {
		int size = idset.size();
		if (size > 200) return true;

		boolean nullable = (client == null);
		if (nullable) client = fetch(top);
		if(client == null) return false;
		
		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				long[] set = client.applyChunkId(1000);
				Logger.info("Launcher.loadChunkIds, chunk identity size: %d", (set == null ? 0 : set.length));
				idset.add(set);
				success = true;
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
	 * load table configure from top site
	 * @param client
	 * @return
	 */
	private boolean loadTable(TopClient client) {
		ArrayList<String> array = new ArrayList<String>();
		// get table configure
		Set<Space> list = spaces.keys();
		for (Space space : list) {
			// save schema name
			String db = space.getSchema().trim().toLowerCase();
			if(!array.contains(db)) {
				array.add(db);
			}
			try {
				Table table = client.findTable(local.getHost(), space.getSchema(), space.getTable());
				if(table == null) {
					Logger.error("Launcher.loadTable, cannot find space '%s'", space);
					return false;
				}
				Logger.info("Launcher.loadTable, save '%s' space table", space);
				spaces.add(space, table);
				local.addSpace(space);
			} catch (VisitException exp) {
				Logger.error(exp);
				return false;
			}
		}
		
		return true;
	}

	/**
	 * get site timeout
	 * @param client
	 * @return
	 */
	private boolean loadTimeout(TopClient client) {
		int timeout = 20;
		boolean nullable = (client == null);
		if (nullable) client = fetch(top);
		if (client == null) return false;

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				timeout = client.getSiteTimeout(local.getFamily());
				this.setSiteTimeout(timeout);
				success = true;
				break;
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
	 * stop log serivce
	 */
	private void stopLog() {
		Logger.stopService();
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		//1. load log service
		boolean success = Logger.loadService(null);
		if (!success) {
			Logger.error("Launcher.init, cannot load log service");
			return false;
		}
		//2. load fixp service
		if (success) {
			Class<?>[] clses = { HomeVisitImpl.class, LogVisitImpl.class, HomeExpressImpl.class };
			success = loadListen(clses, local.getHost());
			Logger.note("Launcher.init, load fixp listen", success);
			if(!success) stopLog();
		}
		//3. load task pool
		if(success) {
			success = loadPool();
			Logger.note("Launcher.init, load pool", success);
			if (!success) {
				stopListen();
				stopLog();
			}
		}
		//4. choose mode(run site or backup site)
		if(success) {
			boolean runstat = this.discuss(local.getHost(), super.backups);
			Logger.info("Launcher.init, current mode is '%s'", (runstat ? "run site" : "backup site"));
			if(runstat) {
				
			} else {
				
			}
		}
		//5. load service
		if(success) {
			TopClient client = fetch(top);
			success = (client != null);
			if(success) {
				success = loadTime(client);
				Logger.note("Launcher.init, set system time", success);
			}
			if(success) {
				success = login(client);
				Logger.note("Launcher.init, login", success);
			}
			if(success) {
				success = loadTimeout(client);
				Logger.note(success, "Launcher.init, set site timeout %d", getSiteTimeout());
			}
			// when run-site status, load table and chunkid set
			if (isRunsite()) {
				if (success) {
					success = loadTable(client);
					Logger.note("Launcher.init, load table", success);
				}
				if (success) {
					success = loadChunkIds(client);
					Logger.note("Launcher.init, load chunk identity", success);
				}
			}
			if(success) {
				success = relogin(client);
				Logger.note("Launcher.init, relogin", success);
			}
			// close top-client
			complete(client);
			// when failed, release all
			if (!success) {
				stopPool();
				stopListen();
				stopLog();
			}
		}

		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// logout
		logout(null);
		// stop task pool
		stopPool();
		// stop listener
		stopListen();
		// stop log service
		stopLog();
		// save configure
		writeResouce();
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("Launcher.process, into...");
		this.refreshEndTime();
		
		while (!super.isInterrupted()) {
			if (isRunsite()) {
				this.watch();
			} else {
				this.stakeout();
			}
		}
		
		Logger.info("Launcher.process, exit");
	}

	protected void swing() {
		if (isMaxSiteTimeout()) {
			refreshEndTime();
			relogin(null);
		} else if (isSiteTimeout()) { // "hello" to top
			hello(local.getFamily(), top);
		}
	}

	/**
	 * when run-site status
	 */
	private void watch() {
		
		while(!isInterrupted()) {
			long end = System.currentTimeMillis() + 1000;
			
			if (super.isLoginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.login(null);
			} else if (super.isReloginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.relogin(null);
			} else {
				swing();
			}
						
			// relogin to top
			if(local.isPublish()) {
				local.setPublish(false);
				relogin(null);
			}
			// apply chunk identity, when empty
			if(idset.isEmpty()) {
				loadChunkIds(null);
			}
			
			if (super.isNoneOperate()) {
				long timeout = end - System.currentTimeMillis();
				if (timeout > 0) delay(timeout);
			}
		}
		
	}

	/**
	 * in not run-site status
	 */
	private void stakeout() {
		long copytime = super.getCopyInterval() * 1000;
		long activetime = super.getActiveInterval() * 1000;
		long endcopy = System.currentTimeMillis() + copytime;
		long endactive = System.currentTimeMillis() + activetime;
		
		Helper helper = new Helper(this);
		helper.start();
		
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
		
		// stop ping
		helper.stop();
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
			Logger.warning("Launcher.smell, cannot find home-site");
			return false;
		}

		// connect run-site (keep udp)
		boolean success = false;
		try {
			tracker = new HomeExpressClient(host.getPacketHost());
			tracker.setRecvTimeout(30);
			tracker.reconnect();
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
			HomeExpressClient client = this.fetch(host.getStreamHost());
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
	 * copy resource data from run-site
	 * @return
	 */
	private boolean backup() {
		if (tracker == null) {
			return false;
		}

		boolean success = false;
		try {
			byte[] data = tracker.chunkid();
			idset.clear();
			this.idset.parseXML(data);

			data = tracker.spaces();
			this.spaces.clear();
			spaces.resolveXML(data);

			// save site address
			allsite.clear();
			SiteHost[] hosts = tracker.findSite(Site.LOG_SITE);
			saveto(hosts, allsite);
			hosts = tracker.findSite(Site.DATA_SITE);
			saveto(hosts, allsite);
			hosts = tracker.findSite(Site.WORK_SITE);
			saveto(hosts, allsite);
			hosts = tracker.findSite(Site.BUILD_SITE);
			saveto(hosts, allsite);
			hosts = tracker.findSite(Site.CALL_SITE);
			saveto(hosts, allsite);
			
			success = true;
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}

		if (success) {
			writeResouce();
		} else {
			this.closeup(tracker);
			tracker = null;

			if(!this.smell()) {
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
	 * active operate
	 * @return
	 */
	private boolean active() {
		if (tracker == null) {
			return false;
		}

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			if (i > 0) this.delay(1000);
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
			
			if(!this.smell()) {
				this.verdict();
			}
		}
		
		return success;
	}
	
	/**
	 * query run-site
	 */
	private void verdict() {		
		while (!super.isInterrupted()) {
			// 询问相邻的网站,指定的地址是否已经失效
			int status = this.evaluate();
			Logger.info("Launcher.verdict, evaluate result: %s", JobCrawler.explain(status));

			if (status == JobCrawler.UNDEFINE) {
				this.delay(5000);
			} else if (status == JobCrawler.EXISTED) {
				break;
			} else if (status == JobCrawler.NOTFOUND) {
				// choose a new run-site
				boolean runstat = this.discuss(local.getHost(), super.backups);
				Logger.info("Launcher.verdict, current mode is '%s'", (runstat ? "run site" : "backup site"));
				// when run-status, relogin to self 
				if (runstat) {
					// notify all job site(log, data, work, build, call)
					this.transfer(this.allsite);
				}
				break;
			}
		}
	}

	/**
	 * check run-site status
	 * @return
	 */
	private int evaluate() {
		long check_time = super.getDetectInterval() * 1000; // milli-second
		// query home site, check run-topsite
		JobCrawler crawler = new JobCrawler();
		int status = crawler.detect(this.allsite, runsite, check_time);
		return status;
	}

	/**
	 * notify all job site, re-login to self
	 * @param hosts
	 */
	protected void transfer(List<SiteHost> hosts) {
		Command cmd = new Command(Request.NOTIFY, Request.TRANSFER_HUB);
		Packet packet = new Packet(cmd);
		packet.addMessage(Key.LOCAL_ADDRESS, local.getHost().toString());

		for (SiteHost host : hosts) {
			SocketHost remote = host.getPacketHost();
			for (int i = 0; i < 3; i++) {
				super.fixpPacket.send(remote, packet);
			}
			this.delay(10);
		}
	}


	private boolean loadResource() {
		boolean success = this.loadIdentify();
		Logger.note("Launcher.loadResource, load chunk identity", success);
		if(success) {
			success = this.loadSpace();
			Logger.note("Launcher.loadResource, load user space", success);
		}
		return success;
	}

	private void writeResouce() {
		this.flushIdentify();
		this.flushSpace();
	}

	/**
	 * 协商,如果是local,返回true. 否则返回false
	 * 
	 * @param local
	 * @param backups
	 * @return
	 */
	private boolean discuss(SiteHost local, List<SiteHost> backups) {
		List<SiteHost> a = new ArrayList<SiteHost>(backups);
		a.add(local);
		SiteHost[] hosts = new SiteHost[a.size()];
		a.toArray(hosts);

		List<SiteHost> records = new ArrayList<SiteHost>();
		int nulls = 0;
		for(SiteHost host : backups) {
			HomeExpressClient client = this.fetch(host.getStreamHost());
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
		
		if (nulls == backups.size() || records.size() == backups.size()) {
			for (SiteHost host : records) {
				if (!host.equals(local)) return false;
			}
			runsite = null;
			this.setRunsite(true);
			return true;
//			return (this.runflag = true);
		}
		return false;
	}
	
	/**
	 * parse local site
	 * @param doc
	 * @return
	 */
	protected SiteHost splitTop(Document doc) {
		XMLocal xml = new XMLocal();
		Element elem = (Element) doc.getElementsByTagName("top-site").item(0);
		String ip = xml.getValue(elem, "ip");
		String tcport = xml.getValue(elem, "tcp-port");
		String udport = xml.getValue(elem, "udp-port");

		try {
			return new SiteHost(InetAddress.getByName(ip), Integer.parseInt(tcport), Integer.parseInt(udport));
		} catch (UnknownHostException e) {
			Logger.error(e);
		}
		return null;
	}

	/**
	 * 加载并且设置本地管理配置
	 * @param filename
	 * @return
	 */
	private boolean loadLocal(String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) {
			Logger.error("Launcher.loadLocal, cannot resolve %s", filename);
			return false;
		}
		// 解析TOP节点服务器地址
		SiteHost host = splitTop(document);
		if (host == null) {
			Logger.error("Launcher.loadLocal, cannot parse top site");
			return false;
		}
		this.top.set(host);
		Logger.info("Launcher.loadLocal, top site: %s", host);
		// 解析当前HOME节点绑定地址
		host = super.splitLocal(document);
		if (host == null) {
			Logger.error("Launcher.loadLocal, cannot parse local site");
			return false;
		}
		local.setHost(host);
		Logger.info("Launcher.loadLocal, local address:%s", host);
		
		// 后备监视HOME节点地址
		if (!super.loadBackups(document)) {
			Logger.error("Launcher.loadLocal, cannot parse backup address set");
			return false;
		}
		
		// HOME节点资源配置目录
		String s = xml.getXMLValue(document.getElementsByTagName("resource-directory"));
		if (!super.createResourcePath(s)) {
			Logger.error("Launcher.loadLocal, cannot create directory %s", s);
			return false;
		}
		
		// 代码位资源存储目录
		s = xml.getXMLValue(document.getElementsByTagName("codepoint-directory"));
		if (s != null && s.length() > 0) {
			CodePointPool.getInstance().setRoot(s);
		}
		
		// 解析并且设置FIXP监视器安全配置
		if(!super.loadSecurity(document)) {
			Logger.error("Launcher.loadLocal, cannot parse security file");
			return false;
		}
		
		// check timeout
		s = xml.getXMLValue(document.getElementsByTagName("sleep-time"));
		int sleepTime = Integer.parseInt(s);
		CallPool.getInstance().setSleep(sleepTime);
		DataPool.getInstance().setSleep(sleepTime);
		LogPool.getInstance().setSleep(sleepTime);
		WorkPool.getInstance().setSleep(sleepTime);
		BuildPool.getInstance().setSleep(sleepTime);
		// login site timeout
		s = xml.getXMLValue(document.getElementsByTagName("site-timeout"));
		int siteTimeout = Integer.parseInt(s);
		CallPool.getInstance().setSiteTimeout(siteTimeout);
		DataPool.getInstance().setSiteTimeout(siteTimeout);
		LogPool.getInstance().setSiteTimeout(siteTimeout);
		WorkPool.getInstance().setSiteTimeout(siteTimeout);
		BuildPool.getInstance().setSiteTimeout(siteTimeout);
		// delete site timeout
		s = xml.getXMLValue(document.getElementsByTagName("delete-timeout"));
		int deleteTimeout = Integer.parseInt(s);
		CallPool.getInstance().setDeleteTimeout(deleteTimeout);
		DataPool.getInstance().setDeleteTimeout(deleteTimeout);
		LogPool.getInstance().setDeleteTimeout(deleteTimeout);
		WorkPool.getInstance().setDeleteTimeout(deleteTimeout);
		BuildPool.getInstance().setDeleteTimeout(deleteTimeout);

		return Logger.loadXML(filename);
	}


	
	private boolean loadIdentify() {
		File file = buildResourceFile(IdentitySet.filename);
		// not found, return true;
		if(!file.exists()) return true;
		byte[] data = readFile(file);
		if(data == null) return false;
		return idset.parseXML(data);
	}

	private boolean flushIdentify() {
		byte[] data = idset.buildXML();
		File file = buildResourceFile(IdentitySet.filename);
		return flushFile(file, data);
	}
	
	private boolean loadSpace() {
		File file = buildResourceFile(UserSpace.filename);
		if (!file.exists()) return true;
		byte[] data = readFile(file);
		if(data == null) return false;
		return spaces.parseXML(data);
	}
	
	/**
	 * flush space data to disk
	 */
	private boolean flushSpace() {
		byte[] data = spaces.buildXML();
		File file = buildResourceFile(UserSpace.filename);
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