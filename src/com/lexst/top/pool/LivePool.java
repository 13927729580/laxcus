/**
 *
 */
package com.lexst.top.pool;

import java.util.*;

import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.site.*;
import com.lexst.site.live.*;
import com.lexst.sql.account.*;
import com.lexst.sql.schema.*;
import com.lexst.top.*;
import com.lexst.top.effect.*;
import com.lexst.util.host.*;

/**
 * TOP节点上，SQLive/SQLive console节点管理池。<br>
 * 管理范围包括:注册/注销/重新注册LIVE节点；定时检查超时节点，自动删除。<br><br>
 * 
 * 与其它节点管理不同的是，如果LIVE节点超时而未向TOP节点发送心跳激活包，TOP节点在达到删除时间后即删除LIVE节点记录
 *
 */
public class LivePool extends ControlPool {
	
	/** 管理池句柄 **/
	private static LivePool selfHandle = new LivePool();

	/** LIVE节点配属数据库表 -> 节点地址集合 */
	private Map<Space, SiteSet> mapSpace = new TreeMap<Space, SiteSet>();

	/** LIVE节点地址 -> LIVE节点配置 **/
	private Map<SiteHost, LiveSite> mapSite = new TreeMap<SiteHost, LiveSite>();

	/** LIVE节点地址 -> LIVE节点最后刷新时间 **/
	private Map<SiteHost, Long> mapTime = new TreeMap<SiteHost, Long>();

	/**
	 * 初始化管理池
	 */
	private LivePool() {
		super();
	}

	/**
	 * 返回LivePool的静态句柄
	 * @return
	 */
	public static LivePool getInstance() {
		return LivePool.selfHandle;
	}
	
	/**
	 * @return
	 */
	public List<SiteHost> gather() {
		ArrayList<SiteHost> array = new ArrayList<SiteHost>();
		array.addAll(mapSite.keySet());
		return array;
	}
	
	/**
	 * LIVE节点注册
	 * @param object
	 * @return
	 */
	public short add(Site object) {
		if (object == null || !object.isLive()) {
			return Response.CLIENT_ERROR;
		}
		LiveSite site = (LiveSite) object;
		SiteHost host = site.getHost();
		
		Logger.info("LivePool.add, live site %s", host);
		
		this.lockSingle();
		try {
			//1. 如果地址已经存在
			if (mapSite.containsKey(host)) {
				Logger.error("LivePool.add, duplicate socket host %s", host);
				return Response.ADDRESS_EXISTED;
			}
			//2. 检查注册账号是否存在
			User user = site.getUser();
			if (!Launcher.getInstance().onUser(user)) {
				Logger.warning("LivePool.add, cannot find '%s'", user.getHexUsername());
				return Response.NOTFOUND;
			}
			//3. 检查重复注册(一个账号只能在一个地址注册)
			for (LiveSite privious : mapSite.values()) {
				if (user.equals(privious.getUser())) {
					Logger.warning("LivePoo.add, '%s' existed!", user.getHexUsername());
					return Response.ACCOUNT_EXISTED;
				}
			}
			// 保存配置
			for(Space space : site.list()) {
				SiteSet set = mapSpace.get(space);
				if(set == null) {
					set = new SiteSet();
					mapSpace.put(space, set);
				}
				set.add(host);
			}
			// 注册登录用户
			mapSite.put(host, site);
			mapTime.put(host, System.currentTimeMillis());
			return Response.ACCEPTED;
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		return Response.SERVER_ERROR;
	}

	/**
	 * LIVE节点注销
	 * @param host
	 * @return
	 */
	public short remove(SiteHost host) {
		Logger.info("LivePool.remove, live site %s", host);

		this.lockSingle();
		try {
			// 删除注册地址
			LiveSite site = mapSite.remove(host);
			if (site == null) {
				return Response.NOTACCEPTED;
			}
			// 删除更新时间
			mapTime.remove(host);
			// 删除所属配置表
			for (Space space : site.list()) {
				SiteSet set = mapSpace.get(space);
				if (set != null) {
					set.remove(site.getHost());
				}
				if (set == null || set.isEmpty()) {
					mapSpace.remove(space);
				}
			}
			return Response.ACCEPTED;
		} catch(Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		return Response.SERVER_ERROR;
	}
	
	/**
	 * LIVE节点重新注册
	 * @param site
	 * @return
	 */
	public short update(Site site) {
		Logger.debug("LivePool.update, relogin %s", site.getHost());

		if (remove(site.getHost()) == Response.ACCEPTED) {
			return this.add(site);
		}
		return Response.NOTACCEPTED;
	}
	
	/**
	 * check site existed
	 * @param host
	 * @return
	 */
	public boolean exists(SiteHost host) {
		super.lockMulti();
		try {
			return mapSite.containsKey(host);
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockMulti();
		}
		return false;
	}

	/**
	 * LIVE节点发送心跳包保持激活
	 * @param host
	 * @return
	 */
	public short refresh(SiteHost host) {
		short code = Response.SERVER_ERROR;
		this.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			// 更新时间
			if (site != null) {
				mapTime.put(host, System.currentTimeMillis());
				code = Response.LIVE_ISEE;
			} else {
				code = Response.NOTLOGIN;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		Logger.debug("LivePool.refresh, live site %s refresh status %d", host, code);
		return code;
	}

	/**
	 *
	 * @param host
	 * @param schema
	 * @return
	 */
	public short createSchema(SiteHost host, Schema schema) {
		if (schema == null) {
			return Response.CLIENT_ERROR;
		}
		
		Logger.info("LivePool.createSchema, database is %s", schema.getName());
		
		super.lockSingle();
		try {
			// 必须是注册账号
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return Response.NOTLOGIN;
			}
			// 如果不是管理员即检查权限
			UserManager manager = Launcher.getInstance().getUserManager();
			if (!manager.isDBA(site.getUser())) {
				Account account = manager.findAccount(site.getUser().getHexUsername());
				if (account == null) {
					return Response.NOTFOUND_ACCOUNT;
				}
				// 检查建立账号的权限
				if (!account.allowCreateSchema()) {
					return Response.REFUSE;
				}
			}
			boolean success = Launcher.getInstance().createSchema(schema);
			if(success) {
				if (!manager.isDBA(site.getUser())) {
					Account account = manager.findAccount(site.getUser().getHexUsername());
					account.addSchema(schema.getName());
				}
				Launcher.getInstance().flushAccount();
				return Response.ACCEPTED;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockSingle();
		}
		return Response.NOTACCEPTED;
	}

	/**
	 * delete a database configure
	 * @param host
	 * @param db
	 * @return
	 */
	public short deleteSchema(SiteHost host, String db) {
		// 先检查主机是否注册,再检查
		if (db == null) {
			return Response.CLIENT_ERROR;
		}
		super.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if(site == null) {
				return Response.NOTLOGIN;
			}
			// check permit
			UserManager manager = Launcher.getInstance().getUserManager();
			if (!manager.isDBA(site.getUser())) {
				Account account = manager.findAccount(site.getUser().getHexUsername());
				if (account == null) {
					return Response.REFUSE;
				}
				// 检查建立账号的权限
				if (!account.allowDropSchema()) {
					return Response.REFUSE;
				}
			}
			boolean success = Launcher.getInstance().deleteSchema(db);
			if(success) {
				if(!manager.isDBA(site.getUser())) {
					Account account = manager.findAccount(site.getUser().getHexUsername());
					account.deleteSchema(db);
				}
				Launcher.getInstance().flushAccount();
				return Response.ACCEPTED;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockSingle();
		}
		return Response.NOTACCEPTED;
	}

	/**
	 * create a account
	 * @param host
	 * @param user
	 * @return
	 */
	public short createUser(SiteHost host, User user) {
		Logger.info("LivePool.createUser, create account: %s", user.getHexUsername());

		this.lockSingle();
		try {
			// 主机必须注册
			LiveSite oldsite = mapSite.get(host);
			if(oldsite == null) {
				return Response.NOTLOGIN;
			}
			UserManager manager = Launcher.getInstance().getUserManager();
			// 不允许建立DBA同名账号
//			if (Arrays.equals(manager.getDBA().getUsername(), user.getUsername())) {
			if (manager.getDBA().equals(user)) {
				// if (manager.getDBA().matchUsername(user.getUsername())) {
				return Response.REFUSE;
			}
			// 用户同名账号是否存在
			Account account = manager.findAccount(user); //user.getHexUsername());
			if (account != null) {
				return Response.ACCOUNT_EXISTED;
			}

			// 如果不是DBA账号，检查账号权限(用户权限由DBA赋予)
			if (!manager.isDBA(oldsite.getUser())) {
				Account oldAccount = manager.findAccount(oldsite.getUser()); //.getHexUsername());
				if (oldAccount == null) {
					return Response.NOTFOUND;
				}
				// 检查是否有权建立账号
				if(!oldAccount.allowCreateUser()) {
					return Response.REFUSE;
				}
			}
			// 建立一个新账号
			account = new Account(user);
			if(manager.addAccount(account)) {
				Launcher.getInstance().flushAccount();
				return Response.ACCEPTED;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		// failed
		return Response.NOTACCEPTED;
	}

	/**
	 * delete account
	 * @param host
	 * @param username
	 * @return
	 */
	public short deleteUser(SiteHost host, String username) {
		Logger.info("LivePool.deleteUser, drop user: %s", username);
		
		User user = new User();
		user.setHexUsername(username);

		this.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if(site == null) {
				return Response.NOTLOGIN;
			}
			UserManager manager = Launcher.getInstance().getUserManager();
			// 如果是DBA账号,不允许删除
			if (Arrays.equals(manager.getDBA().getUsername(), user.getUsername())) {
				// if(manager.getDBA().matchUsername(user.getUsername())) {
				return Response.NOTACCEPTED;
			}
			// 检查被删除账号是否存在
			Account account = manager.findAccount(username);
			if(account == null) {
				return Response.NOTFOUND;
			}
			// 账号是DBA,或者是自己的账号,允许删除
			boolean success = false;
			if (manager.isDBA(site.getUser())) {
				success = manager.deleteAccount(username);
			} else if (Arrays.equals(site.getUser().getUsername(), user.getUsername())) { 
				// site.getUser().matchUsername(user.getUsername())) {
				success = manager.deleteAccount(username);
				// 同时删除注册地址
				mapSite.remove(host);
				mapTime.remove(host);
			}
			if(success) {
				Launcher.getInstance().flushAccount();
				return Response.ACCEPTED;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		return Response.NOTACCEPTED;
	}
	
	/**
	 * 检查用户账号是否存在
	 * @param host
	 * @param username - 16进制数字的字符串
	 * @return
	 */
	public boolean onUser(SiteHost host, String username) {
		Logger.info("LivePool.onUser, username: %s", username);

		User user = new User();
		user.setHexUsername(username);

		super.lockMulti();
		try {
			// 检查站点是否存在
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return false;
			}
			UserManager manager = Launcher.getInstance().getUserManager();
			// 不能检查DBA账号
			if (Arrays.equals(manager.getDBA().getUsername(), user.getUsername())) {
				return false;
			}
			// 检查普通用户账号
			Account account = manager.findAccount(username);
			return account != null;
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockMulti();
		}

		return false;
	}

	/**
	 * modify account user
	 * @param host
	 * @param other
	 * @return
	 */
	public short alterUser(SiteHost host, User other) {
		Logger.info("LivePool.alterUser, alter user: %s", other.getHexUsername());

		// DBA账号,不允许网络修改.
		// 登陆是DBA账号,或者是自己的账号,允许修改
		this.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if(site == null) {
				return Response.NOTLOGIN;
			}
			UserManager manager = Launcher.getInstance().getUserManager();
			// 如果是DBA账号,不允许修改
			if (Arrays.equals(manager.getDBA().getUsername(), other.getUsername())) {
				// if(manager.getDBA().matchUsername(other.getUsername())) {
				return Response.NOTACCEPTED;
			}
			// 检查被删除账号是否存在
			Account account = manager.findAccount(other.getHexUsername());
			if(account == null) {
				return Response.NOTFOUND;
			}

			// 账号是DBA,或者是自己的账号,允许修改
			boolean success = false;
			if (manager.isDBA(site.getUser())) {
				account.setUser(other);
				success = true;
			} else if (Arrays.equals(site.getUser().getUsername(), other.getUsername())) {
				// site.getUser().matchUsername(other.getUsername())) {
				site.getUser().set(other);
				success = true;
			}
			if(success) {
				Launcher.getInstance().flushAccount();
				return Response.ACCEPTED;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		return Response.NOTACCEPTED;
	}
	


	/**
	 * add permit
	 * @param host
	 * @param permit
	 * @return
	 */
	public short addPermit(SiteHost host, Permit permit) {
		Logger.debug("LivePool.addPermit, username '%s'", permit.getUsers().get(0));

		this.lockSingle();
		try {
			// 检查节点是否注册
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return Response.NOTLOGIN;
			}
			UserManager manager = Launcher.getInstance().getUserManager();
			// 如果不是DBA,检查用户权限
			if (!manager.isDBA(site.getUser())) {
				Account account = manager.findAccount(site.getUser().getHexUsername());
				if(account == null) {
					return Response.NOTFOUND_ACCOUNT;
				}
				if(!account.allowGrant()) {
					return Response.REFUSE;
				}
			}
			// 追加管理权限
			List<String> list = permit.getUsers();
			if (!manager.exists(list)) {
				return Response.NOTFOUND_ACCOUNT;
			}
			for (String username : list) {
				Account account = manager.findAccount(username);
				account.add(permit);
			}
			// save account
			Launcher.getInstance().flushAccount();
			return Response.ACCEPTED;
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		
		return Response.SERVER_ERROR;
	}

	/**
	 * delete permit
	 * @param host
	 * @param permit
	 * @return
	 */
	public short deletePermit(SiteHost host, Permit permit) {
		this.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return Response.NOTLOGIN;
			}

			UserManager manager = Launcher.getInstance().getUserManager();
			// 不是DBA,检查操作权限
			if (!manager.isDBA(site.getUser())) {
				Account account = manager.findAccount(site.getUser().getHexUsername());
				if (account == null) {
					return Response.NOTFOUND_ACCOUNT;
				}
				if (!account.allowRevoke()) {
					return Response.REFUSE;
				}
			}
			// 删除管理权限
			List<String> list = permit.getUsers();
			if (!manager.exists(list)) {
				return Response.NOTFOUND_ACCOUNT;
			}
			for (String username : list) {
				Account account = manager.findAccount(username);
				account.remove(permit);
			}

			Launcher.getInstance().flushAccount();
			return Response.ACCEPTED;
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		return Response.SERVER_ERROR;
	}

	/**
	 * craete a table space
	 * @param host
	 * @param table
	 * @return
	 */
	public short createTable(SiteHost host, Table table) {
		if(table == null) {
			return Response.CLIENT_ERROR;
		}
		
		Space space = table.getSpace();
		Logger.info("LivePool.createTable, create table space '%s'", space);

		super.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return Response.NOTLOGIN;
			}
			UserManager manager = Launcher.getInstance().getUserManager();
			// 如果不是管理员账号,检查权限
			if(!manager.isDBA(site.getUser())) {
				Account account = manager.findAccount(site.getUser().getHexUsername());
				if (account == null) {
					return Response.NOTFOUND_ACCOUNT;
				}
				// 检查建立账号的权限
				if (!account.allowCreateTable(space.getSchema())) {
					return Response.REFUSE;
				}
			}
			// 检查表,如果表存在,不允许
			Table old = Launcher.getInstance().findTable(space);
			if(old != null) {
				return Response.TABLE_EXISTED;
			}
			// check database
			Dict dict = Launcher.getInstance().getDict();
			if (dict.findSchema(space.getSchema()) == null) {
				return Response.NOTFOUND_SCHEMA;
			}
			// create table
			boolean success = Launcher.getInstance().createSpace(table);
			if(success) {
				if(!manager.isDBA(site.getUser())) {
					Account account = manager.findAccount(site.getUser().getHexUsername());
					account.addSpace(space);
				}
				Launcher.getInstance().flushAccount();
				// return id
				return Response.ACCEPTED;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockSingle();
		}
		return Response.NOTACCEPTED;
	}

	/**
	 * delete table
	 * @param host
	 * @param space
	 * @return
	 */
	public short deleteTable(SiteHost host, Space space) {
		Logger.info("LivePool.deleteTable, drop table '%s' from %s", space, host);
		if (space == null) return Response.CLIENT_ERROR;

		super.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return Response.NOTLOGIN;
			}

			UserManager manager = Launcher.getInstance().getUserManager();
			// 如果不是管理员账号,检查权限
			if(!manager.isDBA(site.getUser())) {
				Account account = manager.findAccount(site.getUser().getHexUsername());
				if (account == null) {
					return Response.REFUSE;
				}
				// 检查建立账号的权限
				if (!account.allowDropTable(space.getSchema())) {
					return Response.REFUSE;
				}
			}
			// 检查表,如果表存在,不允许
			Table old = Launcher.getInstance().findTable(space);
			if(old == null) {
				return Response.NOTFOUND_TABLE;
			}
			// check database
			Dict dict = Launcher.getInstance().getDict();
			if (dict.findSchema(space.getSchema()) == null) {
				return Response.NOTFOUND_SCHEMA;
			}
			boolean success = Launcher.getInstance().deleteSpace(space);
			if (success) {
				if(!manager.isDBA(site.getUser())) {
					Account account = manager.findAccount(site.getUser().getHexUsername());
					account.deleteSpace(space);
				}
				Launcher.getInstance().flushAccount();
				return Response.ACCEPTED;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		return Response.NOTACCEPTED;
	}

	/**
	 * find table
	 * @param host
	 * @param space
	 * @return
	 */
	public Table findTable(SiteHost host, Space space) {
		Logger.info("LivePool.findTable, space \'%s\', site host: %s", space, host);
		
		super.lockMulti();
		try {
			if (!mapSite.containsKey(host)) {
				Logger.error("LivePool.findTable, cannot find login site %s, when find table", host);
				return null;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockMulti();
		}
		// return table
		Table table = Launcher.getInstance().findTable(space);
		return table;
	}

	public String[] getSchemas(SiteHost host) {
		Logger.info("LivePool.getSchemas, site host %s", host);

		ArrayList<String> a = new ArrayList<String>();
		this.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return null;
			}
			UserManager manager = Launcher.getInstance().getUserManager();
			Dict dict = Launcher.getInstance().getDict();
			if(manager.isDBA(site.getUser())) {
				Set<String> keys = dict.keys();
				for (String dbname : keys) {
					Schema db = dict.findSchema(dbname);
					if (db != null) a.add(db.getName());
				}
			} else {
				Account account = manager.findAccount(site.getUser().getHexUsername());
				if (account == null) return null;
				Set<String> key = account.schemaKeys();
				for (String dbname : key) {
					Schema db = dict.findSchema(dbname);
					if (db != null) a.add(db.getName());
				}
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		if(a.isEmpty()) return null;
		String[] s = new String[a.size()];
		return a.toArray(s);
	}

	/**
	 * get table configure
	 * @param host
	 * @return
	 */
	public Table[] getTables(SiteHost host) {
		Logger.info("LivePool.getTables, site %s", host);

		ArrayList<Table> a = new ArrayList<Table>(32);
		this.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return null;
			}
			UserManager manager = Launcher.getInstance().getUserManager();
			Dict dict = Launcher.getInstance().getDict();
			// 如果是管理员账号,取全部数据库表
			if (manager.isDBA(site.getUser())) {
				// 取全部配置
				Set<String> keys = dict.keys();
				for (String dbname : keys) {
					Schema db = dict.findSchema(dbname);
					a.addAll(db.listTable());
				}
			} else {
				// 找到匹配的账号,取出匹配的表
				Account account = manager.findAccount(site.getUser().getHexUsername());
				Set<String> key = account.schemaKeys();
				for (String db : key) {
					List<Space> list = account.findSpaces(db);
					for (Space space : list) {
						Table table = dict.findTable(space);
						a.add(table);
					}
				}
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}

		if(a.isEmpty())	return null;
		Table[] s = new Table[a.size()];
		return a.toArray(s);
	}

	/**
	 * find space
	 *
	 * @param host
	 * @return
	 */
	public Permit[] getPermits(SiteHost host) {
		Logger.info("LivePool.getPermits, site %s", host);

		ArrayList<Permit> a = new ArrayList<Permit>();
		this.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return null;
			}
			String name = site.getUser().getHexUsername();
			UserManager manager = Launcher.getInstance().getUserManager();
			Account account = manager.findAccount(name);
			if (account == null) {
				return null;
			}
			Collection<Permit> list = account.list();
			if (list != null && !list.isEmpty()) {
				a.addAll(list);
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		if (a.isEmpty()) return null;
		Permit[] all = new Permit[a.size()];
		return a.toArray(all);
	}

	/**
	 * check user identified
	 * @param host
	 * @return
	 */
	public short checkIdentified(SiteHost host) {
		Logger.info("LivePool.checkIdnetified, site %s", host);

		short type = Response.UNIDENTIFIED;
		this.lockSingle();
		try {
			LiveSite site = mapSite.get(host);
			if (site == null) {
				return Response.NOTLOGIN;
			}
			UserManager manager = Launcher.getInstance().getUserManager();
			if (manager.isDBA(site.getUser())) {
				type = Response.SQL_ADMIN;
			} else {
				String name = site.getUser().getHexUsername();
				Account account = manager.findAccount(name);
				if (account != null) {
					type = Response.SQL_USER;
				}
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			this.unlockSingle();
		}
		return type;
	}

	/**
	 * 检查超时的节点
	 */
	private void checkTimeout() {
		int size = mapSite.size();
		if (size == 0) return;

		ArrayList<SiteHost> timeouts = new ArrayList<SiteHost>(size);
		super.lockSingle();
		try {
			long now = System.currentTimeMillis();
			for (SiteHost host : mapTime.keySet()) {
				Long value = mapTime.get(host);
				if (value == null || now - value.longValue() >= getDeleteTimeout()) {
					timeouts.add(host);
				}
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockSingle();
		}
		
		// 删除严重超时的节点
		for (SiteHost host : timeouts) {
			this.remove(host);
		}
	}

	private void check() {
		this.checkTimeout();
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		mapTime.clear();
		mapSite.clear();
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
		Logger.info("LivePool.process, into ...");
		while (!isInterrupted()) {
			this.delay(1000);
			if (isInterrupted()) break;
			this.check();
		}
		Logger.info("LivePool.process, exit");
	}
}