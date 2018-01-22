/**
 *
 */
package com.lexst.live.pool;

import java.io.*;
import java.security.interfaces.*;
import java.util.*;

import org.w3c.dom.*;

import com.lexst.fixp.*;
import com.lexst.live.*;
import com.lexst.log.client.*;
import com.lexst.remote.client.top.*;
import com.lexst.security.*;
import com.lexst.site.*;
import com.lexst.site.live.*;
import com.lexst.sql.account.*;
import com.lexst.sql.parse.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;
import com.lexst.util.res.*;
import com.lexst.visit.*;
import com.lexst.xml.*;
import com.lexst.pool.JobPool;

/**
 * SQLive / SQLive console 调用的数据配置池
 *
 */
public class TouchPool extends JobPool implements SQLChooser {

	private static TouchPool selfHandle = new TouchPool();
	
	private LiveListener listener;

	/** 当前主机地址 */
	private LiveSite local;

	/** 连接的TOP主机地址 **/
	private SiteHost remote;

	// into check status
	private boolean into;
	/** SQLive 节点超时间 **/
	private int siteTimeout;
	
	/** 账号权限集合 **/
	private List<Permit> permits = new ArrayList<Permit>();

	/** 数据库名称集合 **/
	private Set<String> schemas = new HashSet<String>();

	/** 数据库表集合 */
	private Map<Space, Table> tables = new HashMap<Space, Table>();

	/** 登录用户的账号级别(数据库管理DBA或者普通用户) */
	private short userRank;
	
	private long iseeTime;
	
	/**
	 * 初始化连接池
	 */
	private TouchPool() {
		// cannot check
		into = false;
		// 默认超时时间
		siteTimeout = 15000;
		// 账号级别默认为未定义
		userRank = 0;
	}

	/**
	 * @return
	 */
	public static TouchPool getInstance() {
		return TouchPool.selfHandle;
	}
	
	/*
	 * 根据数据库表名查找数据库表配置
	 * @see com.lexst.sql.parse.SQLChooser#findTable(com.lexst.sql.schema.Space)
	 */
	@Override
	public Table findTable(Space space) {
		Table table = null;
		super.lockMulti();
		try {
			table = this.tables.get(space);
		} catch (Throwable e) {
			Logger.error(e);
		} finally {
			super.unlockMulti();
		}
		
		// 如果是管理员，允许查找所有数据库表
		if (isDBA() && table == null) {
			TopClient client = this.fetch(false);
			try {
				table = client.findTable(local.getHost(), space);
			} catch (VisitException e) {
				Logger.error(e);
			} catch (Throwable e) {
				Logger.fatal(e);
			} finally {
				this.complete(client);
			}
		}
		
		return table;
	}

	/*
	 * 检查数据库表是否存在
	 * @see com.lexst.sql.parse.SQLChooser#onTable(com.lexst.sql.schema.Space)
	 */
	@Override
	public boolean onTable(Space space) {
		boolean success = false;
		super.lockMulti();
		try {
			success = (tables.get(space) != null);
		} catch (Throwable e) {
			Logger.error(e);
		} finally {
			super.unlockMulti();
		}
		
		// 非管理员，返回结果；是管理员，连接TOP服务器查询
		if (isDBA() && !success) {
			TopClient client = this.fetch(false);
			try {
				Table table = client.findTable(local.getHost(), space);
				success = (table != null);
			} catch (VisitException e) {
				Logger.error(e);
			} catch (Throwable e) {
				Logger.fatal(e);
			} finally {
				this.complete(client);
			}
		}

		return success;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.parse.SQLChooser#onSchema(java.lang.String)
	 */
	@Override
	public boolean onSchema(String schema) {
		boolean success = false;
		super.lockMulti();
		try {
			for (String s : schemas) {
				success = schema.equalsIgnoreCase(s);
				if (success) break;
			}
		} catch (Throwable e) {
			Logger.error(e);
		} finally {
			super.unlockMulti();
		}

		// 如果是管理员并且没有找到，去TOP节点查询
		if (isDBA() && !success) {
			TopClient client = this.fetch(false);
			try {
				Schema result = client.findSchema(local.getHost(), schema);
				success = (result != null);
			} catch (VisitException e) {
				Logger.error(e);
			} catch (Throwable e) {
				Logger.fatal(e);
			} finally {
				this.complete(client);
			}
		}

		return success;
	}
	
	/*
	 * 检查用户账号<br>
	 * DBA可以检查所有用户的账号，并且可以建立账号。<br>
	 * 普通账号只能检查自己的账号，没有建立账号的权限。<br>
	 * 
	 * @see com.lexst.sql.parse.SQLChooser#onUser(java.lang.String)
	 */
	@Override
	public boolean onUser(String username) {
		User user = new User();
		user.setTextUsername(username);
		
		byte[] origin = user.getUsername();
		byte[] dest = local.getUser().getUsername();

		//1. 不是DBA，只能检查自己的账号名称
		if (!this.isDBA()) {
			return Arrays.equals(origin, dest);
		} else {
			// 如果是DBA，不允许和自己账号相同
			if (Arrays.equals(origin, dest)) {
				return false;
			}
			//2. 启动TOP连接，检查账号是否存在(16进制字符串)
			TopClient client = this.fetch(false);
			try {
				return client.onUser(local.getHost(), user.getHexUsername());
			} catch (VisitException e) {
				Logger.error(e);
			} catch (Throwable e) {
				Logger.fatal(e);
			} finally {
				this.complete(client);
			}
		}

		return false;
	}
	
	/**
	 * 设置当前绑定的地址
	 * @param site
	 */
	public void setLocal(LiveSite site) {
		this.local = site;
	}

	public LiveSite getLocal() {
		return this.local;
	}
	

	
	public void setRemote(SiteHost host) {
		this.remote = new SiteHost(host);
	}
	public SiteHost getRemote() {
		return this.remote;
	}
	
	public void setLiveListener(LiveListener listener) {
		this.listener = listener;
	}
	
	public List<String> listSchema() {
		return new ArrayList<String>(schemas);
	}
	
	public boolean containSchema(String name) {
		for(String s : schemas) {
			if(s.equalsIgnoreCase(name)) return true;
		}
		return false;
	}
	
	public Map<Space, Table> getTables() {
		return this.tables;
	}


	
	public List<Table> listTable() {
		ArrayList<Table> a = new ArrayList<Table>();
		a.addAll(tables.values());
		return a;
	}
	
	public boolean existsTable(Space space) {
		return findTable(space) != null;
	}
	
	public void resetUser() {
		userRank = 0;
		permits.clear();
		schemas.clear();
		tables.clear();
	}

	/**
	 * reset all
	 */
	public void reset() {
		into = false;
		resetUser();
	}

	public void setInto(boolean b) {
		this.into = b;
	}
	public boolean isInto() {
		return this.into;
	}

	public boolean addPermit(TopClient client, Site local, Permit permit) {
		boolean nullable = (client == null);
		if (nullable) client = fetch();
		boolean success = false;
		try {
			
			if (client != null) {
				success = client.addPermit(local.getHost(), permit);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return success;
	}

	public boolean deletePermit(TopClient client, Site local, Permit permit) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if (nullable) client = fetch();
			success = client.deletePermit(local.getHost(), permit);
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return success;
	}

	public boolean createUser(TopClient client, Site local, User user) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if(nullable) client = fetch();
			if (client != null) {
				success = client.createUser(local.getHost(), user);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return success;
	}

	public boolean deleteUser(TopClient client, Site local, String username) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				success = client.deleteUser(local.getHost(), username);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return success;
	}

	public boolean alterUser(TopClient client, Site local, User user) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				success = client.alterUser(local.getHost(), user);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return success;
	}

	/**
	 * create a database
	 * @param client
	 * @param local
	 * @param schema
	 * @return
	 */
	public boolean createSchema(TopClient client, Site local, Schema schema) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				success = client.createSchema(local.getHost(), schema);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);

		Logger.note(success, "TouchPool.createSchema, create database %s", schema.getName());
		return success;
	}
	
	public Schema findSchema(TopClient client, Site local, String db) {
		boolean nullable = (client == null);
		Schema schema = null;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				schema = client.findSchema(local.getHost(), db);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return schema;
	}
	
	/**
	 * @param client
	 * @param local
	 * @return
	 */
	public Schema[] findAllSchema(TopClient client, Site local) {
		boolean nullable = (client == null);
		List<Schema> array = new ArrayList<Schema>();
		try {
			if (nullable) client = fetch();
			if(client != null) {
				String[] names = client.getSchemas(local.getHost());
				for (int i = 0; names != null && i < names.length; i++) {
					Schema schema = client.findSchema(local.getHost(), names[i]);
					if (schema != null) array.add(schema);
				}
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		if (array.isEmpty()) return null;
		Schema[] a = new Schema[array.size()];
		return array.toArray(a);
	}

	/**
	 * delete a database
	 * @param client
	 * @param local
	 * @param db
	 * @return
	 */
	public boolean deleteSchema(TopClient client, Site local, String db) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				success = client.deleteSchema(local.getHost(), db);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		
		Logger.note(success, "TouchPool.deleteSchema, delete database %s", db);
		return success;
	}

	/**
	 * create a database table
	 * @param table
	 * @return
	 */
	public boolean createTable(TopClient client, Site local, Table table) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if (nullable) client = fetch();
			success = client.createTable(local.getHost(), table);
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		
		if (success) {
			Space space = table.getSpace();
			tables.put(space, table);
		}
		
		Logger.note(success, "TouchPool.createTable, create '%s'", table.getSpace());
		return success;
	}
	
	public Table findTable(TopClient client, Site local, Space space) {
		boolean nullable = (client == null);
		Table table = null;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				table = client.findTable(local.getHost(), space.getSchema(), space.getTable());
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);

		return table;
	}

	/**
	 * delete a database table
	 * @param space
	 * @return
	 */
	public boolean deleteTable(TopClient client, Site local, Space space) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				success = client.deleteTable(local.getHost(), space.getSchema(), space.getTable());
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		
		Logger.note(success, "TouchPool.deleteTable, delete '%s'", space);
		return success;
	}

	/**
	 * get all database configure for a user
	 * @return
	 */
	public int getSchemas(TopClient client, Site local) {
		boolean nullable = (client == null);
		int count = 0;
		try {
			if(nullable) client = fetch();
			String[] names = client.getSchemas(local.getHost());
			for (int i = 0; names != null && i < names.length; i++) {
				if (names[i] == null) continue;
				this.schemas.add(names[i]);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		
		return count;
	}
	
	/**
	 * get all table configure for a user
	 * @return
	 */
	public int getTables(TopClient client, Site local) {
		boolean nullable = (client == null);
		int count = 0;
		try {
			if(nullable) client = fetch();
			if(client != null) {
				Table[] all = client.getTables(local.getHost());
				for (int i = 0; all != null && i < all.length; i++) {
					if (all[i] != null) {
						tables.put(all[i].getSpace(), all[i]);
					}
				}
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return count;
	}
	
	/**
	 * @param client
	 * @param local
	 * @param space
	 * @param size
	 * @return
	 */
	public boolean setChunkSize(TopClient client, Site local, Space space, int size) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				success = client.setChunkSize(space.getSchema(), space.getTable(), size);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return success;
	}
	
	/**
	 * 通知TOP节点，定义一个表的数据重构时间
	 * @param client
	 * @param local
	 * @param space
	 * @param columnId
	 * @param type
	 * @param time
	 * @return
	 */
	public boolean setRebuildTime(TopClient client, Site local, Space space, short columnId, int type, long time) {
		boolean nullable = (client == null);
		boolean success = false;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				success = client.setRebuildTime(space.getSchema(), space.getTable(), columnId, type, time);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if(nullable) complete(client);
		return success;
	}

	/**
	 * @param space
	 * @return
	 */
	public SiteHost[] findHomeSite(TopClient client, Site local, Space space) {
		boolean nullable = (client == null);
		SiteHost[] hosts = null;
		try {
			if (nullable) client = fetch();
			if (client != null) {
				hosts = client.findHomeSite(space.getSchema(), space.getTable());
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return hosts;
	}

	public short checkIdentified(TopClient client, Site local) {
		boolean nullable = (client == null);
		try {
			if (nullable) client = fetch();
			userRank = client.checkIdentified(local.getHost());
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);

		return userRank;
	}
	
	public Address[] rebuild(TopClient client, Site local, Space space, short columnId, Address[] addresses) throws VisitException {
		boolean nullable = (client == null);
		Address[] s = null;
		if (nullable) client = fetch();
		if (client != null) {
			s = client.rebuild(local.getHost(), space, columnId, addresses);
		}
		if (nullable) complete(client);
		return s;
	}
	
	public Address[] loadIndex(TopClient client, Site local, Space space, Address[] addresses) throws VisitException {
		boolean nullable = (client == null);
		Address[] s = null;
		if (nullable) client = fetch();
		if (client != null) {
			s = client.loadIndex(local.getHost(), space, addresses);
		}
		if (nullable) complete(client);
		return s;
	}
	
	public Address[] stopIndex(TopClient client, Site local, Space space, Address[] addresses) throws VisitException {
		boolean nullable = (client == null);
		Address[] s = null;
		if (nullable) client = fetch();
		if (client != null) {
			s = client.stopIndex(local.getHost(), space, addresses);
		}
		if (nullable) complete(client);
		return s;
	}

	/**
	 * load chunk data to memory
	 * @param client
	 * @param local
	 * @param space
	 * @param addresses
	 * @return
	 * @throws VisitException
	 */
	public Address[] loadChunk(TopClient client, Site local, Space space, Address[] addresses) throws VisitException {
		boolean nullable = (client == null);
		Address[] s = null;
		if (nullable) client = fetch();
		if (client != null) {
			s = client.loadChunk(local.getHost(), space, addresses);
		}
		if (nullable) complete(client);
		return s;
	}

	/**
	 * release chunk data
	 * @param client
	 * @param local
	 * @param space
	 * @param addresses
	 * @return
	 * @throws VisitException
	 */
	public Address[] stopChunk(TopClient client, Site local, Space space, Address[] addresses) throws VisitException {
		boolean nullable = (client == null);
		Address[] s = null;
		if (nullable) client = fetch();
		if (client != null) {
			s = client.stopChunk(local.getHost(), space, addresses);
		}
		if (nullable) complete(client);
		return s;
	}
	
	/**
	 * send "build task" command to site
	 * @param client
	 * @param local
	 * @param naming
	 * @param addresses
	 * @return
	 * @throws VisitException
	 */
	public Address[] buildTask(TopClient client, Site local, String naming, Address[] addresses) throws VisitException {
		boolean nullable = (client == null);
		Address[] s = null;
		if (nullable) client = fetch();
		try {
			if (client != null) {
				s = client.buildTask(local.getHost(), naming, addresses);
			}
		} catch (VisitException exp) {
			throw exp;
		} catch (Throwable exp) {
			throw new VisitException(exp);
		} finally {
			if (nullable) complete(client);
		}
		return s;
	}
	
	public long showChunkSize(TopClient client, SiteHost local, String schema, int type, Address[] sites) throws VisitException {
		long chunksize = -1L;
		boolean nullable = (client == null);
		if (nullable) client = fetch();
		try {
			if(client != null) {
				chunksize = client.showChunkSize(local, schema, type, sites);
			}
		} catch (VisitException exp) {
			throw exp;
		} catch (Throwable exp) {
			throw new VisitException(exp);
		} finally {
			if (nullable) complete(client);
		}
		return chunksize;
	}

	public long showChunkSize(TopClient client, SiteHost local, Space space, int siteType, Address[] sites) throws VisitException{
		long chunksize = -1L;
		boolean nullable = (client == null);
		if (nullable) client = fetch();
		try {
			if(client != null) {
				chunksize = client.showChunkSize(local, space.getSchema(), space.getTable(), siteType, sites);
			}
		} catch (VisitException exp) {
			throw exp;
		} catch (Throwable exp) {
			throw new VisitException(exp);
		} finally {
			if (nullable) complete(client);
		}
		return chunksize;
	}
	
	/**
	 * 显示命名任务
	 * @param client
	 * @param local
	 * @param tag
	 * @param sites
	 * @return
	 * @throws VisitException
	 */
	public List<TaskAddress> showTask(TopClient client, SiteHost local, String tag, Address[] sites) throws VisitException {
		boolean nullable = (client == null);
		if (nullable) client = fetch();

		String s = null;
		try {
			if(client != null) {
				s = client.showTask(local, tag, sites);
			}
		} catch (VisitException exp) {
			throw exp;
		} catch (Throwable exp) {
			throw new VisitException(exp);
		} finally {
			if (nullable) complete(client);
		}
		
		// resolve data
		// format: [naming site]\r\n[naming site]\r\n[naming site]
		List<TaskAddress> array = new ArrayList<TaskAddress>();
		try {
			BufferedReader reader = new BufferedReader(new StringReader(s));
			while (true) {
				String line = reader.readLine();
				if (line == null) break;
				String[] elems = line.split(" ");
				if (elems != null && elems.length == 2) {
					array.add(new TaskAddress(elems[0], elems[1]));
				}
			}
			reader.close();
		} catch (IOException exp) {
			Logger.error(exp);
		}
		
		// result set
		return array;
	}
	
	public SiteHost[] showSite(TopClient client, Site local, int site, Address[] froms) throws VisitException {
		boolean nullable = (client == null);
		SiteHost[] hosts = null;
		if (nullable) client = fetch();
		try {
			if (client != null) {
				hosts = client.showSite(site, froms);
			}
		} catch (VisitException exp) {
			throw exp;
		} catch (Throwable exp) {
			throw new VisitException(exp);
		} finally {
			if (nullable) complete(client);
		}
		return hosts;
	}
	
	/**
	 * 是管理员账号
	 * @return
	 */
	public boolean isDBA() {
		return userRank == Response.SQL_ADMIN;
	}

	/**
	 * 是普通用户账号
	 * @return
	 */
	public boolean isUser() {
		return userRank == Response.SQL_USER;
	}
	
	/**
	 * 申请一个TOP节点连接(SOCKET流/包模式二选一)
	 * @param stream
	 * @return
	 */
	private TopClient fetch(boolean stream) {
		if (remote == null) {
			Logger.error("TouchPool.fetch, top site is null");
			return null;
		}

		SocketHost host = (stream ? remote.getStreamHost() : remote.getPacketHost());
		TopClient client = new TopClient(stream, host);
		try {
			client.reconnect();
			return client;
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		return null;
	}

	/**
	 * 申请一个TOP节点连接(SOCKET流模式)
	 * @return
	 */
	private TopClient fetch() {
		return fetch(true);
	}

	/**
	 * 关闭SOCKET，释放与TOP节点的连接
	 * @param client
	 */
	private void complete(TopClient client) {
		if (client == null) return;
		try {
			client.exit();
			client.close();
		} catch (IOException exp) {
			
		}
	}
	
	/**
	 * 根据配置参数生成RSA公钥
	 * @return
	 */
	private RSAPublicKey generate() {
		ResourceLoader loader = new ResourceLoader();
		byte[] b = loader.findStream("conf/terminal/key/rsa/rsakey.public");
		if (b == null) return null;

		XMLocal xml = new XMLocal();
		Document doc = xml.loadXMLSource(b);

		NodeList list = doc.getElementsByTagName("code");
		Element elem = (Element)list.item(0);

		String modulus = xml.getXMLValue(elem.getElementsByTagName("modulus"));
		String exponent = xml.getXMLValue(elem.getElementsByTagName("exponent"));

		try {
			return SecureGenerator.buildRSAPublicKey(modulus, exponent);
		} catch ( SecureException e) {
			Logger.error(e);
		}
		return null;
	}
	
	/**
	 * login to top site
	 * @param server
	 * @param local
	 * @return
	 */
	public boolean login(SiteHost server, LiveSite local) {
		this.remote = new SiteHost(server);
		
		TopClient client = fetch(false);
		if(client == null) {
			Logger.error("TouchPool.login, cannot connect %s", remote);
			return false;
		}
		
		boolean success = false;
		try {
			//1. safe check
			String algo = local.getAlgorithm();
			if (algo != null) {
				RSAPublicKey key = generate();
				if (key == null) {
					this.complete(client);
					return false;
				}
				byte[] pwd = local.getUser().getPassword();
				boolean safe = client.initSecure(key, algo, pwd);
				Logger.note("TouchPool.login, init security", safe);
				if (!safe) {
					this.complete(client);
					return false;
				}
			}

			//2. login to top server
			success = client.login(local);
			Logger.note(success, "TouchPool.login, login to %s", remote);
			if (success) {
				int second = client.getSiteTimeout(local.getFamily());
				siteTimeout = second * 1000;
				Logger.info("TouchPool.login, site timeout %d", second);

				checkIdentified(client, local);
				int ret1 = getPermits(client, local);
				int ret2 = getSchemas(client, local);
				int ret3 = getTables(client, local);
				if (ret1 >= 0 && ret2 >= 0 && ret3 >= 0) {

				}
				this.into = true;
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch(Throwable exp) {
			Logger.fatal(exp);
		}
		
		complete(client);

		return success;
	}

	/**
	 * logout from home site
	 * @return
	 */
	public boolean logout(LiveSite site) {
		TopClient client = fetch(false);
		if (client == null) {
			Logger.error("TouchPool.logout, cannot connect %s", remote);
			return false;
		}

		boolean success = false;
		try {
			boolean safe = true;
			String algo = site.getAlgorithm();
			if (algo != null) {
				byte[] pwd = site.getUser().getPassword();
				RSAPublicKey key = this.generate();
				if(key == null) return false;
				
				safe = client.initSecure(key, algo, pwd);
				Logger.note("TouchPool.logout, init security", safe);
			}
			if (safe) {
				success = client.logout(site.getFamily(), site.getHost());
				Logger.note(success, "TouchPool.logout, logout from %s", remote);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		this.complete(client);
		if (success) {
			this.reset();
		}
		return success;
	}

	/**
	 * apply all options
	 * @return
	 */
	public int getPermits(TopClient client, Site local) {
		boolean nullable = (client == null);
		// clear old
		permits.clear();
		// get table space
		try {
			if (nullable) client = fetch();
			Permit[] auths = client.getPermits(local.getHost());
			if (auths != null) {
				for (int i = 0; i < auths.length; i++) {
					permits.add(auths[i]);
				}
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (nullable) complete(client);
		return permits.size();
	}

	public void replyActive() {
		iseeTime = System.currentTimeMillis();
		this.wakeup();
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// logout service, when into check status
		if (this.into) {
			this.logout(local);
		}
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
		long endtime = 0L;
		iseeTime = System.currentTimeMillis();
		while (!isInterrupted()) {
			delay(1000);
			if (isInterrupted()) break;

			// when into check status
			if (into) {
				if (System.currentTimeMillis() >= endtime) {
					Logger.debug("TouchPool.process, active %s", remote);
					listener.active(1, remote.getPacketHost());
					endtime = System.currentTimeMillis() + siteTimeout;
				}
				if(System.currentTimeMillis() - iseeTime >= siteTimeout * 2) {
					listener.active(3, remote.getPacketHost());
					endtime = System.currentTimeMillis() + siteTimeout;
				}
				if(System.currentTimeMillis() - iseeTime >= siteTimeout * 5) {
					// notify launcher, site timeout, close service, exit!
					this.listener.disconnect();
					this.into = false;
				}
			}
		}
	}

}