/**
 *
 */
package com.lexst.call.pool;

import java.io.*;
import java.util.*;

import com.lexst.algorithm.init.*;
import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.pool.site.*;
import com.lexst.remote.client.data.*;
import com.lexst.sql.chunk.*;
import com.lexst.sql.column.Column;
import com.lexst.sql.conduct.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.util.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;

/**
 * 数据操作池，执行与DATA节点的数据操作。
 * 
 *
 */
public class DataPool extends JobPool {

	private static DataPool selfHandle = new DataPool();
	
//	private Map<SiteHost, ClientSet> mapClient = new TreeMap<SiteHost, ClientSet>();
//
//	/** 表名 -> 索引集合 */
//	private Map<Space, IndexModule> mapModule = new TreeMap<Space, IndexModule>();
//
//	/** chunkid -> 节点地址集合 */
//	private Map<Long, SiteSet> mapChunk = new TreeMap<Long, SiteSet>();
//
//	/** 表名 -> 节点地址集合 */
//	private Map<Space, SiteSet> mapSpace = new HashMap<Space, SiteSet>(16);
//	
//	/** diffuse task naming -> data host set */
//	private Map<Naming, SiteSet> mapNaming = new TreeMap<Naming, SiteSet>();
//
//	/** 表名 -> 表配置  */
//	private Map<Space, Table> mapTable = new TreeMap<Space, Table>();
//	
//	/** 表名 -> DATA主节点地址  */
//	private Map<Space, SiteHost> mapPrime = new HashMap<Space, SiteHost>();
//	
//	/** HOME节点通知，更新当前的全部DATA节点记录  **/
//	private boolean refresh;
//
//	private long localIP = 0L;
//
//	private long number = 0L;
	
	/**
	 * default
	 */
	private DataPool() {
		super();
//		this.refresh = false;
	}

	/**
	 * 返回静态句柄
	 * @return
	 */
	public static DataPool getInstance() {
		return DataPool.selfHandle;
	}
	
	/**
	 * 建立与DATA节点连接
	 * 
	 * @param host
	 * @param stream
	 * @return
	 */
	private DataClient createClient(SiteHost host, boolean stream) {
		boolean success = false;
		// 连接DATA节点
		SocketHost address = (stream ? host.getStreamHost() : host.getPacketHost());
		DataClient client = new DataClient(address);
		try {
			client.reconnect();
			success = true;
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (!success) {
			client.close();
			return null;
		}
		
		return client;
	}
	
	/**
	 * 连接DATA节点
	 * @param host
	 * @return
	 */
	private DataClient createClient(SiteHost host) {
		return createClient(host, true);
	}

	/**
	 * conduct操作前，初始化任务，分配后继资源配置
	 * @param conduct
	 * @return
	 */
	private Conduct initTask(Conduct conduct) throws InitTaskException {
		// 取初始化对象，如果没有是错误!
		InitObject init = conduct.getInit();
		if (init == null) {
			throw new RuntimeException("init error!");
		}

		// 根据命名取得任务实例
		Naming naming = init.getNaming();
		InitTask task = InitTaskPool.getInstance().find(naming);
		if (task == null) {
			Logger.error("DataPool.initTask, cannot find naming:%s", naming);
			return null;
		}
		
		// 初始化任务命名执行
		return task.init(conduct);
	}
	
//	/**
//	 * direct|conduct identify
//	 * @return
//	 */
//	private synchronized long nextIdentity() {
//		if (localIP == 0L) {
//			localIP = callInstance.getLocal().getHost().getIPValue();
//			localIP <<= 32;
//		}
//		if (number >= 0x0FFFFFFFL) number = 0;
//		number++;
//		return localIP | number;
//	}
	
//	/**
//	 * 更新状态，通知线程从HOME节点取新数据
//	 */
//	public void refresh() {
//		this.refresh = true;
//		wakeup();
//	}
//	
//	/**
//	 * 根据表名，查找匹配的索引矩阵
//	 * 
//	 * @param space
//	 * @return
//	 */
//	public IndexModule findModule(Space space) {
//		super.lockMulti();
//		try {
//			return this.mapModule.get(space);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//		return null;
//	}
//	
//	/**
//	 * 根据命名，查找对应的DATA节点地址集合
//	 * 
//	 * @param naming
//	 * @return
//	 */
//	public SiteSet findDataNodes(Naming naming) {
//		super.lockMulti();
//		try {
//			return mapNaming.get(naming);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//		return null;
//	}
//	
//	/**
//	 * 根据chunkid，找到对应的DATA节点地址集合
//	 * @param chunkid
//	 * @return
//	 */
//	private SiteSet findDataNodes(long chunkid) {
//		super.lockMulti();
//		try {
//			return mapChunk.get(chunkid);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//		return null;
//	}
//
//	/**
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
//					DataClient client = (DataClient) set.get(i);
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
//	private DataClient findClient(SiteHost host, boolean stream) {
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
//			DataClient client = (DataClient) set.lockNext();
//			if (client != null) return client;
//			// when not lock-client
//			if (set.size() >= ClientSet.LIMIT) {
//				client = (DataClient) set.next();
//				client.locking();
//				return client;
//			}
//		}
//		
//		boolean success = false;
//		// connect to host
//		SocketHost address = (stream ? host.getTCPHost() : host.getUDPHost());
//		DataClient client = new DataClient(address);
//		try {
//			client.reconnect();
//			success = true;
//		} catch (IOException exp) {
//			Logger.error(exp);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
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
//	
//	private DataClient findClient(SiteHost host) {
//		return findClient(host, true);
//	}
//
//	/**
//	 * find prime data client
//	 * @param space
//	 * @return
//	 */
//	private DataClient findPrime(Space space) {
//		DataHost current = null, previous = null;
//		SiteSet set = null;
//		super.lockMulti();
//		try {
//			// before address
//			previous = (DataHost)mapPrime.get(space);
//			set = mapSpace.get(space);	
//		} catch(Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//		
//		if (set == null) {
//			Logger.error("DataClient.findPrime, cannot find space '%s'", space);
//			return null;
//		}
//		
//		int size = set.size();
//		Logger.debug("DataPool.findPrime, '%s' host size %d", space, size);
//		for (int i = 0; i < size; i++) {
//			DataHost host = (DataHost) (previous != null && i == 0 ? set.next(previous) : set.next());
//			if (host != null && host.isPrime()) {
//				current = host;
//				break;
//			}
//		}
//
//		if (current != null) {
//			super.lockSingle();
//			try {
//				mapPrime.put(space, current);
//			} catch (Throwable exp) {
//				Logger.fatal(exp);
//			} finally {
//				super.unlockSingle();
//			}
//			// get client handle
//			return findClient(current);
//		}
//		
//		return null;
//	}

//	private Table findTable(Space space) {
//		Table table = null;
//		super.lockMulti();
//		try {
//			table = mapTable.get(space);
//		} catch(Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//		if(table != null) return table;
//
//		HomeClient client = super.bring(false);
//		if(client == null) return null;
//		try {
//			table = client.findTable(space);
//		} catch (VisitException exp) {
//			Logger.error(exp);
//		}
//		this.complete(client);
//		if (table == null) return null;
//
//		super.lockSingle();
//		try {
//			mapTable.put(space, table);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//		return table;
//	}
	
	/**
	 * 根据查询条件进行搜索
	 * 提示: 
	 * 1. table 可以自定义,或者由系统,不需要与table.space保持一致
	 * 2. select 中的 space 与 dc 的space 保持一致
	 * @param select
	 */
	public byte[] select(Select select) {
		
//		if (select.getGroupBy() != null && select.getOrderBy() != null) {
//
//		} else if (select.getGroupBy() != null) {
//			// GROUPBY 先于ORDERBY 执行
//		} else if (select.getOrderBy() != null) {
//			// 启动diffuse/aggregate分布查询
//			//1. 根据表名，找到对应的，执行aggregate任务的WORK主机地址
//			//2. 拿到全部分片(CHAR,SCHAR,WCHAR用代码位，其它从xxxIndexChart中取)
//			//3. 根据分片和WORK主机地址，对存在的数据平均分片
//			//4. 向DATA节点发起ORDERBY的DIFFUSE任务(不走普通的SELECT路径)
//			//5. 收到DATA节点上的反馈，通知对应的WORK主机，向DATA节点请求数据
//			//6. 接收来自WORK主机的结果数据流，返回给调用端
//		}
		
		// 如果有GROUP BY 或者 ORDER BY操作，生成Conduct，执行分布计算
		if (select.getGroupBy() != null || select.getOrderBy() != null) {			
			// 生成conduct对象
			Conduct conduct = new Conduct();
			// 设置初始化命名对象，由它去分派具体实现
			conduct.setInit(new InitObject("SYSTEM_SELECT_INIT"));
			// 设置FROM对象，保存SELECT
			FromInputObject input = new FromInputObject();
			input.addSelect(select);
			FromObject from = new FromObject();
			from.setInput(input);
			conduct.setFrom(from);
			// 去接口中执行配置参数分配，如数据分片，再执行计算
			return this.conduct(conduct);
		}
		
		long time = System.currentTimeMillis();

		// 检查删除锁定,如果存在,必须等待,直到结束
		Space space = select.getSpace();
//		IndexModule module = null;
//		super.lockMulti();
//		try {
//			module = this.mapModule.get(space);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
		
//		IndexModule module = findModule(space);
		
		IndexModule module = FromPool.getInstance().findIndexModule(space);
		if (module == null) {
			Logger.error("DataPool.select, cannot find index module for '%s'", space);
			return null;
		}
		// 根据WHERE语句，检索匹配的chunkid
		ChunkSet tokens = new ChunkSet();
		int count = module.find(select.getCondition(), tokens);
		if (count < 0) {
			Logger.warning("DataPool.select, cannot find match chunk");
			return null;
		}
		
		// 按照CHUNKID，找到匹配的主机地址
		HashMap<SiteHost, ChunkSet> map = new HashMap<SiteHost, ChunkSet>();
		for(long chunkId : tokens.list()) {
			//debug code, start
			Logger.debug("DataPool.select, check chunk id [%x - %d]", chunkId, chunkId);
			//debug code, end
			
//			SiteSet set = null;
//			super.lockMulti();
//			try {
//				set = mapChunk.get(chunkId);
//			} catch (Throwable exp) {
//				Logger.fatal(exp);
//			} finally {
//				super.unlockMulti();
//			}
			
//			SiteSet set = this.findDataNodes(chunkId);
			SiteSet set = FromPool.getInstance().findFromSites(chunkId);
			if(set == null) {
				Logger.error("DataPool.select, cannot find chunk id [%d - %x]", chunkId, chunkId);
				continue;
			}
			SiteHost host = set.next();
			if(host == null) {
				Logger.error("DataPool.select, cannot find host %s", host);
				continue;
			}
			// 分片
			ChunkSet idset = map.get(host);
			if(idset == null) {
				idset = new ChunkSet();
				map.put(host, idset);
			}
			idset.add(chunkId);
		}

		// data节点连接委托代理器
		DataTrustor finder = new DataTrustor();
		// 预定义客户端连接数
		finder.setJobs(map.size());
		// 线程工作完成立即退出
		finder.setKeepThread(false);

		for(SiteHost host : map.keySet()) {
			ChunkSet idset = map.get(host);
			// 查找/连接data节点，返回句柄
			DataClient client = this.createClient(host);
			if(client == null) {
				Logger.error("DataPool.select, cannot find client:%s", host);
				break;
			}
			Select clone_select = (Select)select.clone();
			long[] chunkIds = idset.toArray();
			clone_select.setChunkids(chunkIds);
			// 立即执行
			finder.send(client, clone_select);
		}

		Logger.debug("DataPool.select, pre-select usedtime %d", System.currentTimeMillis() - time);

//		// error occurred
//		if (map.size() != finder.size()) {
//			finder.disconnect(false);
//			return null;
//		}
		
//		// start query
//		finder.launch();
		
		// wait and get data
		finder.waiting();
		byte[] data = finder.data();
		
		Logger.debug("DataPool.select, complete select usedtime:%d", System.currentTimeMillis()-time);
		
		return data;
	}

	/**
	 * @param delete
	 * @return
	 */
	public long delete(Delete delete) {
		Space space = delete.getSpace();
		Logger.error("DataPool.delete, space is '%s'", space);
		
		//1. 将所有 "主节点" 主机全部锁定. 后面检索只限于对"主节点"删除.删除完毕后,再删除副节点
		// 检查删除锁定,如果存在,必须等待,直到结束
//		IndexModule module = null;
//		super.lockMulti();
//		try {
//			module = mapModule.get(space);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
		
//		IndexModule module = findModule(space);
		
		// 检索索引模块
		IndexModule module = FromPool.getInstance().findIndexModule(space);
		if (module == null) {
			Logger.error("DataPool.delete, cannot find '%s' index module", space);
			return -1;
		}
		ChunkSet tokens = new ChunkSet();
		int count = module.find(delete.getCondition(), tokens);
		if (count < 0) {
			Logger.warning("DataPool.delete, cannot find match chunk");
			return 0;
		}

		// 按照chunkid,找到匹配的主机地址
		HashMap<SiteHost, ChunkSet> map = new HashMap<SiteHost, ChunkSet>();
		for(long chunkId : tokens.list()) {
//			SiteSet set = null;
//			super.lockMulti();
//			try {
//				set = mapChunk.get(chunkId);
//			} catch (Throwable exp) {
//				Logger.fatal(exp);
//			} finally {
//				super.unlockMulti();
//			}
			
//			SiteSet set = this.findDataNodes(chunkId);
			
			SiteSet set = FromPool.getInstance().findFromSites(chunkId);
			if(set == null) {
				Logger.warning("DataPool.delete, cannot find chunk id by %d", chunkId);
				continue;
			}
			SiteHost host = set.next();
			if(host == null) {
				Logger.warning("DataPool.delete, cannot find host");
				continue;
			}
			// 分片
			ChunkSet idset = map.get(host);
			if(idset == null) {
				idset = new ChunkSet();
				map.put(host, idset);
			}
			idset.add(chunkId);
		}

		DataTrustor finder = new DataTrustor();
		// 预定义客户端连接数
		finder.setJobs(map.size());
		// 线程工作完成立即退出
		finder.setKeepThread(false);

		for (SiteHost host : map.keySet()) {
			ChunkSet idset = map.get(host);
			// 查找/连接DATA节点
			DataClient client = createClient(host);
			if(client == null) {
				Logger.error("DataPool.delete, cannot connect %s", host);
				break;
			}
			Delete clone_delete = (Delete)delete.clone();
			long[] chunkIds = idset.toArray();
			clone_delete.setChunkids(chunkIds);
			// 立即执行
			finder.send(client, clone_delete);
		}
		
//		if (map.size() != finder.size()) {
//			finder.disconnect(true);
//			return -1;
//		}

//		// start delete
//		finder.launch();
		
		// wait and get data
		finder.waiting();
		// delete count
		long deleteItems = finder.getItems();
		
		Logger.debug("DataPool.delete, delete count:%d", deleteItems);
		
		return deleteItems;
	}

	/**
	 * "SQL INSERT"，插入一条记录到DATA节点
	 * 
	 * @param insert
	 * @param sync (同步或者异步)
	 * @return
	 */
	public int insert(Insert insert, boolean sync) {
		Space space = insert.getSpace();
		Logger.debug("DataPool.insert, flush to '%s'", space);
				
		// 找到主节点，建立连接
		SiteSet set = FromPool.getInstance().findPrime(space);
		if(set == null || set.isEmpty()) {
			Logger.error("DataPool.insert, cannot find site by %s", space);
			return -1;
		}
		// 循环调用DATA节点地址，平衡负载
		SiteHost host = set.next();
		DataClient client = createClient(host);
		
		if (client == null) {
			Logger.error("DataPool.insert, cannot find client for '%s'", space);
			return -1;
		}
		Logger.debug("DataPool.insert, flusth '%s' to %s", space, client.getRemote());
		// 转成数据流，写入DATA节点
		byte[] data = insert.build();
		int stamp = client.insert(data, sync);
		if(stamp > 0) {
			CodePointCollector.getInstance().push(insert);
		}
		
		return stamp;
	}

	/**
	 * "SQL INSERT"，写入一批记录到数据节点
	 * 
	 * @param inject
	 * @param sync (同步或者异步)
	 * @return
	 */
	public int inject(Inject inject, boolean sync) {
		Space space = inject.getSpace();
		
//		// 申请一个主节点的DATA主机
//		DataClient client = findPrime(space);
		
		// 查找主节点，启动连接
		SiteSet set = FromPool.getInstance().findPrime(space);
		if(set == null || set.isEmpty()) {
			Logger.error("DataPool.inject, cannot find prime site by %s", space);
			return -1;
		}
		SiteHost host = set.next();
		DataClient client = this.createClient(host);
		
		if(client == null) {
			Logger.error("DataPool.inject, space '%s', cannot find client!", space);
			return -1;
		}
		Logger.debug("DataPool.inject, flush '%s' to %s", space, client.getRemote());
		// 转成数据流，写入DATA节点
		byte[] data = inject.build();
		int stamp = client.insert(data, sync);
		// 提取文本首字符的代码位(UTF16)
		if(stamp > 0) {
			CodePointCollector.getInstance().push(inject);
		}
		return stamp;
	}

	/**
	 * update row 
	 * @param update
	 * @return
	 */
	public long update(Update update) {
		Space space = update.getSpace();
		Condition condi = update.getCondition();
		
//		Table table = findTable(space);
		Table table = FromPool.getInstance().findTable(space);
		if (table == null) {
			Logger.error("DataPool.update, cannot find '%s' table", space);
			return -1;
		}
		
		Logger.debug("DataPool.update, space is %s", space);

		//1. 将所有 "主节点" 主机全部锁定. 后面检索只限于对"主节点"删除.删除完毕后,再删除副节点
		// 检查删除锁定,如果存在,必须等待,直到结束
//		IndexModule module = null;
//		super.lockMulti();
//		try {
//			module = mapModule.get(space);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
		
//		IndexModule module = findModule(space);
		
		IndexModule module = FromPool.getInstance().findIndexModule(space);
		if (module == null) {
			Logger.error("DataPool.update, cannot find '%s' index module", space);
			return -1;
		}
		ChunkSet tokens = new ChunkSet();
		int count = module.find(condi, tokens);
		if (count < 0) {
			Logger.warning("DataPool.update, cannot find match chunk");
			return 0;
		}

		// 按照chunkid,找到匹配的主机地址 
		HashMap<SiteHost, ChunkSet> map = new HashMap<SiteHost, ChunkSet>();
		for(long chunkId : tokens.list()) {
//			SiteSet set = null;
//			super.lockMulti();
//			try {
//				set = mapChunk.get(chunkId);
//			} catch (Throwable exp) {
//				Logger.fatal(exp);
//			} finally {
//				super.unlockMulti();
//			}
			
//			SiteSet set = this.findDataNodes(chunkId);
			
			SiteSet set = FromPool.getInstance().findFromSites(chunkId);
			if(set == null) {
				Logger.warning("DataPool.update, cannot find chunk id by %d", chunkId);
				continue;
			}
			SiteHost host = set.next();
			if(host == null) {
				Logger.warning("DataPool.update, cannot find host");
				continue;
			}
			// 分片
			ChunkSet id_set = map.get(host);
			if(id_set == null) {
				id_set = new ChunkSet();
				map.put(host, id_set);
			}
			id_set.add(chunkId);
		}

		DataTrustor finder = new DataTrustor();
		
		finder.setJobs(map.size());
		finder.setKeepThread(false);
		
		for (SiteHost host : map.keySet()) {
			ChunkSet idset = map.get(host);
			
			DataClient client = createClient(host);
			if(client == null) {
				Logger.error("DataPool.update, cannot connect %s", host);
				break;
			}
			Delete delete = new Delete(space);
			delete.setSnatch(true);
			delete.setCondition(condi);
			delete.setChunkids(idset.list());
			
			// 立即执行
			finder.send(client, delete);
		}
		
//		if(map.size() != finder.size()) {
//			finder.disconnect(true);
//			return -1;
//		}
//
//		// execute job
//		finder.launch();
		
		// 等待工作完成
		finder.waiting();
		
		Inject inject = new Inject(table);
		List<Column> values = update.values();

		byte[] data = finder.data();
		
		Logger.debug("DataPool.update, delete data len:%d", (data==null ? -1 : data.length));
		
		long deleItems = Numeric.toLong(data, 0, 8);
		// skip "item" tag(see DataDelegate), offset begin is:8
		for (int off = 8; off < data.length;) {
			Row row = new Row();
			int len = row.resolve(table, data, off, data.length - off);
			if(len < 1) {
				Logger.error("DataPool.update, resolve failed, offset:%d", off);
				break;
			}
			off += len;
			// replace column
			for(Column column : values) {
				row.replace(column);
			}
			inject.add(row);
		}

		// insert item
		boolean sync = false;
		int insertItems = this.inject(inject, sync);
		if (insertItems > 0) {
			return (sync ? insertItems : (int) deleItems);
		}
		return -1;
	}
	
//	/**
//	 * 执行DC计算，没有被过滤的chunkid集合
//	 * @param dc
//	 * @return
//	 */
//	public byte[] direct(Direct dc) {
//		return direct(dc, null);
//	}
	
//	/**
//	 * 执行DC计算
//	 * 
//	 * @param direct
//	 * @param ignoreChunkids - 被过滤的chunkid集合
//	 * @return
//	 */
//	public byte[] direct(Direct direct, long[] ignoreChunkids) {
//		// 设置运行ID，此值参数唯一
//		direct.setRunid(nextIdentity());
//
//		// 初始化各阶段参数
//		direct = (Direct)this.initTask(direct);
//		if(direct == null) {
//			Logger.error("DataPool.direct, init error!");
//			return null;
//		}
//		
//		// 确实WORK节点地址
//		ToObject tobj = direct.getTo();
//		Collection<SiteHost> list = tobj.listSites();
//		// 如果没有定义WORK节点地址，在这里分配
//		if(list.isEmpty()) {
//			int sites = tobj.getSites();
//			Naming naming = tobj.getTaskNaming();
//			Logger.debug("DataPool.dc, aggregate naming: %s", naming);
//			
//			List<SiteHost> all = WorkPool.getInstance().find(naming);
//			if (all == null || all.isEmpty()) {
//				Logger.error("DataPool.dc, cannot find worksite by '%s'", naming);
//				return null;
//			}
//			
//			if (sites < 1 || sites < all.size()) sites = all.size();
//			for (int i = 0; i < sites; i++) {
//				tobj.addSite(all.get(i));
//			}
//		}
//		
//		// 如果没有"SQL SELECT"语句, 直接连接一个命名主机
//		FromObject from = direct.getFrom();
//		if (from.countSelect() > 0) {
//			return direct_select(direct, ignoreChunkids);
//		} else {
//			return direct_notselect(direct);
//		}
//	}
	
//	/**
//	 * 执行DC计算
//	 * 
//	 * @param dc
//	 * @param ignoreChunkids
//	 * @return
//	 */
//	private byte[] direct_select(Direct dc, long[] ignoreChunkids) {
//		DataTrustor trustor = new DataTrustor();
//		int allSites = 0;
//
//		FromObject from = dc.getFrom();
//		List<Select> selects = from.getSelects();
//		for(Select select : selects) {
//			Space space = select.getSpace();
//			
//			// 查找检索集合
//			IndexModule module = this.findModule(space);
//			if (module == null) {
//				Logger.fatal("DataPool.dc_select, cannot find index module for '%s'", space);
//				return null;
//			}
//			ChunkSet tokens = new ChunkSet();
//			int count = module.find(select.getCondition(), tokens);
//			if (count <= 0) {
//				Logger.warning("DataPool.dc_select, cannot find chunk identity");
//				return null;
//			}
//			
//			// 删除冗余的chunkid
//			if(ignoreChunkids != null) {
//				tokens.remove(ignoreChunkids);
//				if(tokens.isEmpty()) {
//					Logger.warning("DataPool.dc_select, null chunkid set");
//					return null;
//				}
//			}
//
//			// 按照chunkid,找到匹配的主机地址
//			Map<SiteHost, ChunkSet> map = new HashMap<SiteHost, ChunkSet>();
//			for(long chunkid : tokens.list()) {
//				SiteSet set = this.findDataNodes(chunkid);
//				if(set == null) {
//					Logger.error("DataPool.dc_select, cannot find chunk id [%d - %x]", chunkid, chunkid);
//					continue;
//				}
//				SiteHost host = set.next();
//				if(host == null) {
//					Logger.error("DataPool.dc_select, cannot find host %s", host);
//					continue;
//				}
//				// 分割chunkid
//				ChunkSet id_set = map.get(host);
//				if(id_set == null) {
//					id_set = new ChunkSet();
//					map.put(host, id_set);
//				}
//				id_set.add(chunkid);
//			}
//			
//			if(map.isEmpty()) {
//				Logger.warning("DataPool.dc_select, cannot find host!");
//				return null;
//			}
//			allSites += map.size();
//			
//			int siteIndex = 1;
//			for(SiteHost host : map.keySet()) {
//				ChunkSet id_set = map.get(host);
//				// find data-client
//				DataClient client = findClient(host);
//				if(client == null) {
//					Logger.error("DataPool.dc_select, cannot find client:%s", host);
//					break;
//				}
//				Select clone_select = (Select) select.clone();
//				long[] chunkIds = id_set.toArray();
//				clone_select.setChunkids(chunkIds);
//				Direct clone_dc = (Direct)dc.clone();
//				
//				clone_dc.getFrom().clearSelects();
//				clone_dc.getFrom().addSelect(clone_select);
//				clone_dc.getFrom().setIndex(siteIndex++);
////				clone_dc.setFromSelect(clone_select);
////				clone_dc.defineFromIndex(siteIndex++);
//				trustor.add(client, clone_dc);
//			}
//		}
//		
//		if(allSites != trustor.size()) {
//			Logger.error("DataPool.dc_select, not match, exit!");
//			trustor.disconnect(true);
//			return null;
//		}
//		
//		// 停止任务
//		trustor.launch();
//		// 等待任务完全结束
//		trustor.waiting();
//		// 取DC结果
//		byte[] data = trustor.data();
//		return data;
//
////		Select select = dc.getFromSelect();
////		Space space = select.getSpace();
//////		IndexModule module = null;
//////		super.lockMulti();
//////		try {
//////			module = mapModule.get(space);
//////		} catch (Throwable exp) {
//////			Logger.fatal(exp);
//////		} finally {
//////			super.unlockMulti();
//////		}
////		
////		IndexModule module = findModule(space);
////		if (module == null) {
////			Logger.fatal("DataPool.dc_select, cannot find index module for '%s'", space);
////			return null;
////		}
////		ChunkIdentitySet tokens = new ChunkIdentitySet();
////		int count = module.find(select.getCondition(), tokens);
////		if (count <= 0) {
////			Logger.warning("DataPool.dc_select, cannot find chunk identity");
////			return null;
////		}
////		
////		// 删除冗余的chunkid
////		if(filteChunkIds != null) {
////			tokens.remove(filteChunkIds);
////			if(tokens.isEmpty()) {
////				Logger.warning("DataPool.dc, null chunkid set");
////				return null;
////			}
////		}
////
////		// 按照chunkid,找到匹配的主机地址
////		Map<SiteHost, ChunkIdentitySet> map = new HashMap<SiteHost, ChunkIdentitySet>();
////		for(long chunkId : tokens.list()) {
////			SiteSet set = null;
////			super.lockMulti();
////			try {
////				set = mapChunk.get(chunkId);
////			} catch (Throwable exp) {
////				Logger.fatal(exp);
////			} finally {
////				super.unlockMulti();
////			}
////			if(set == null) {
////				Logger.error("DataPool.dc, cannot find chunk id [%d - %x]", chunkId, chunkId);
////				continue;
////			}
////			SiteHost host = set.next();
////			if(host == null) {
////				Logger.error("DataPool.dc, cannot find host %s", host);
////				continue;
////			}
////			// 分割chunkid
////			ChunkIdentitySet id_set = map.get(host);
////			if(id_set == null) {
////				id_set = new ChunkIdentitySet();
////				map.put(host, id_set);
////			}
////			id_set.add(chunkId);
////		}
////		
////		int fromsites = map.size();
////		if(fromsites == 0) {
////			Logger.warning("DataPool.dc, cannot find host!");
////			return null;
////		}
////		dc.defineFromSites(fromsites);
////		DataTrustor finder = new DataTrustor(fromsites);
////		
////		int siteIndex = 1;
////		for(SiteHost host : map.keySet()) {
////			ChunkIdentitySet id_set = map.get(host);
////			// find data-client
////			DataClient client = findClient(host);
////			if(client == null) {
////				Logger.error("DataPool.dc, cannot find client:%s", host);
////				break;
////			}
////			Select clone_select = (Select) select.clone();
////			long[] chunkIds = id_set.toArray();
////			clone_select.setChunkids(chunkIds);
////			DC clone_dc = (DC)dc.clone();
////			clone_dc.setFromSelect(clone_select);
////			clone_dc.defineFromIndex(siteIndex++);
////			finder.add(client, clone_dc);
////		}
////
////		if(dc.getDefineFromSites() != finder.size()) {
////			Logger.error("DataPool.dc, not match, exit!");
////			finder.discontinue(true);
////			return null;
////		}
////		// start query
////		finder.execute();
////		// wait and get data
////		finder.waiting();
////		// dc result
////		byte[] data = finder.data();
////		return data;
//	}

//	private byte[] direct_notselect(Direct dc) {
//		FromObject from = dc.getFrom();
//		//1. 找到需要数量的命名主机
//		int from_sites = from.getSites(); // dc.getFromSites();
//		if(from_sites < 1) from_sites = 1;
//		
////		Naming naming = new Naming(dc.getFromNaming());
////		SiteSet set = null;
////		super.lockMulti();
////		try {
////			set = mapNaming.get(naming);
////		} catch (Throwable exp) {
////			Logger.fatal(exp);
////		} finally {
////			super.unlockMulti();
////		}
//		
//		Naming naming = from.getTaskNaming();
//		SiteSet set = this.findDataNodes( naming );
//		
//		if (set == null) {
//			Logger.error("DataPool.dc_notselect, cannot find naming:%s", naming);
//			return null;
//		} else if (set.size() < from_sites) {
//			Logger.error("DataPool.dc_notselect, site missing!");
//			return null;
//		}
//		
//		ArrayList<SiteHost> hosts = new ArrayList<SiteHost>();
//		for (int i = 0; i < from_sites; i++) {
//			hosts.add(set.next());
//		}
//		
//		//2. 启动代理服务
////		dc.defineFromSites(from_sites);
//		
//		int siteIndex = 1;
//		DataTrustor trustor = new DataTrustor(from_sites);
//		for (SiteHost host : hosts) {
//			DataClient client = findClient(host);
//			if (client == null) {
//				Logger.error("DataPool.dc_notselect, cannot connect %s", host);
//				break;
//			}
//			Direct clone_dc = (Direct)dc.clone();
//			// 为每个克隆DC的FROM对象分配一个序列号
//			clone_dc.getFrom().setIndex(siteIndex);
////			clone_dc.defineFromIndex(siteIndex++);
//			// 加入代理服务队列
//			trustor.add(client, clone_dc);
//			siteIndex++;
//		}
//		
//		if(from_sites != trustor.size()) {
//			Logger.error("DataPool.dc_notselect, not match, exit!");
//			trustor.disconnect(true);
//			return null;
//		}
//
//		// 启动
//		trustor.launch();
//		// 等待查询完成
//		trustor.waiting();
//		// 取DC数据
//		byte[] data = trustor.data();
//		return data;
//	}

//	/**
//	 * @param dc
//	 * @param filteChunkids
//	 * @return
//	 */
//	public byte[] dc2(DC dc, long[] filteChunkids) {
//		// 设置运行ID，此值参数唯一
//		dc.setRunid(nextIdentity());
//
//		// 如果没有定义work地址,在这里分配
//		List<SiteHost> list = dc.listToAddress();
//		
//		if (list.isEmpty()) {
//			int sites = dc.getToSites();
//			String naming = dc.getToNaming();
//			Logger.debug("DataPool.dc, aggregate naming: '%s'", naming);
//
//			List<SiteHost> all = WorkPool.getInstance().find(naming);
//			if (all == null || all.isEmpty()) {
//				Logger.error("DataPool.dc, cannot find worksite by '%s'", naming);
//				return null;
//			}
//
//			if (sites < 1 || sites < all.size()) sites = all.size();
//			for (int i = 0; i < sites; i++) {
//				dc.addToAddress(all.get(i));
//			}
//		}
//
//		Select select = dc.getFromSelect();
//		if(select != null) { // 如果没有"select"语句, 直接连接一个命名主机
//			return dc_select(dc, filteChunkids);
//		} else {
//			return dc_notselect(dc);
//		}
//	}
	
//	private byte[] dc_notselect2(DC dc) {
//		//1. 找到需要数量的命名主机
//		int from_sites = dc.getFromSites();
//		if(from_sites < 1) from_sites = 1;
//		Naming naming = new Naming(dc.getFromNaming());
//		SiteSet set = null;
//		super.lockMulti();
//		try {
//			set = mapNaming.get(naming);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//		if (set == null) {
//			Logger.error("DataPool.dc_notselect, cannot find naming:%s", naming);
//			return null;
//		} else if (set.size() < from_sites) {
//			Logger.error("DataPool.dc_notselect, site missing!");
//			return null;
//		}
//		
//		ArrayList<SiteHost> hosts = new ArrayList<SiteHost>();
//		for (int i = 0; i < from_sites; i++) {
//			hosts.add(set.next());
//		}
//		
//		//2. 在克隆DC中给每台主机分配一个索引号
//		dc.defineFromSites(from_sites);
//		DataDelegate finder = new DataDelegate(from_sites);
//
//		int siteIndex = 1;
//		for (SiteHost host : hosts) {
//			DataClient client = findClient(host);
//			if (client == null) {
//				Logger.error("DataPool.dc_notselect, cannot connect %s", host);
//				break;
//			}
//			DC clone_dc = (DC)dc.clone();
//			clone_dc.defineFromIndex(siteIndex++);
//			finder.add(client, clone_dc);
//		}
//		
//		if(dc.getDefineFromSites() != finder.size()) {
//			Logger.error("DataPool.dc_notselect, not match, exit!");
//			finder.discontinue(true);
//			return null;
//		}
//
//		// start query
//		finder.execute();
//		// wait and get data
//		finder.waiting();
//		// dc result
//		byte[] data = finder.data();
//		return data;
//	}

//	/**
//	 * "dc" jobs
//	 * @param dc
//	 * @param filteChunkIds
//	 * @return
//	 */
//	private byte[] dc_select(DC dc, long[] filteChunkIds) {
//		Select select = dc.getFromSelect();
//		Space space = select.getSpace();
////		IndexModule module = null;
////		super.lockMulti();
////		try {
////			module = mapModule.get(space);
////		} catch (Throwable exp) {
////			Logger.fatal(exp);
////		} finally {
////			super.unlockMulti();
////		}
//		
//		IndexModule module = findModule(space);
//		if (module == null) {
//			Logger.fatal("DataPool.dc_select, cannot find index module for '%s'", space);
//			return null;
//		}
//		ChunkIdentitySet tokens = new ChunkIdentitySet();
//		int count = module.find(select.getCondition(), tokens);
//		if (count <= 0) {
//			Logger.warning("DataPool.dc_select, cannot find chunk identity");
//			return null;
//		}
//		
//		// 删除冗余的chunkid
//		if(filteChunkIds != null) {
//			tokens.remove(filteChunkIds);
//			if(tokens.isEmpty()) {
//				Logger.warning("DataPool.dc, null chunkid set");
//				return null;
//			}
//		}
//
//		// 按照chunkid,找到匹配的主机地址
//		Map<SiteHost, ChunkIdentitySet> map = new HashMap<SiteHost, ChunkIdentitySet>();
//		for(long chunkId : tokens.list()) {
//			SiteSet set = null;
//			super.lockMulti();
//			try {
//				set = mapChunk.get(chunkId);
//			} catch (Throwable exp) {
//				Logger.fatal(exp);
//			} finally {
//				super.unlockMulti();
//			}
//			if(set == null) {
//				Logger.error("DataPool.dc, cannot find chunk id [%d - %x]", chunkId, chunkId);
//				continue;
//			}
//			SiteHost host = set.next();
//			if(host == null) {
//				Logger.error("DataPool.dc, cannot find host %s", host);
//				continue;
//			}
//			// 分割chunkid
//			ChunkIdentitySet id_set = map.get(host);
//			if(id_set == null) {
//				id_set = new ChunkIdentitySet();
//				map.put(host, id_set);
//			}
//			id_set.add(chunkId);
//		}
//		
//		int fromsites = map.size();
//		if(fromsites == 0) {
//			Logger.warning("DataPool.dc, cannot find host!");
//			return null;
//		}
//		dc.defineFromSites(fromsites);
//		DataTrustor finder = new DataTrustor(fromsites);
//		
//		int siteIndex = 1;
//		for(SiteHost host : map.keySet()) {
//			ChunkIdentitySet id_set = map.get(host);
//			// find data-client
//			DataClient client = findClient(host);
//			if(client == null) {
//				Logger.error("DataPool.dc, cannot find client:%s", host);
//				break;
//			}
//			Select clone_select = (Select) select.clone();
//			long[] chunkIds = id_set.toArray();
//			clone_select.setChunkids(chunkIds);
//			DC clone_dc = (DC)dc.clone();
//			clone_dc.setFromSelect(clone_select);
//			clone_dc.defineFromIndex(siteIndex++);
//			finder.add(client, clone_dc);
//		}
//
//		if(dc.getDefineFromSites() != finder.size()) {
//			Logger.error("DataPool.dc, not match, exit!");
//			finder.discontinue(true);
//			return null;
//		}
//
//		// start query
//		finder.execute();
//		// wait and get data
//		finder.waiting();
//		// dc result
//		byte[] data = finder.data();
//		return data;
//	}

	/**
	 * 异步分布计算
	 * 
	 * @param conduct
	 * @return
	 */
	public byte[] conduct(Conduct conduct) {
		// 初始化计算，分配各阶段任务参数对具体执行对象
		try {
			conduct = this.initTask(conduct);
		} catch (InitTaskException e) {
			Logger.error(e);
			return null;
		} catch (Throwable e) {
			Logger.fatal(e);
			return null;
		}
		
		FromOutputObject object = conduct.getFrom().getOutput();
		int sites = object.phases();
		
		DataTrustor trustor = new DataTrustor();
		// 预定义客户端连接数
		trustor.setJobs(sites);
		// 线程工作完成立即退出
		trustor.setKeepThread(false);

		for (int index = 0; index < sites; index++) {
			FromPhase phase = object.getPhase(index);
			SiteHost host = phase.getRemote();
			// 查找/连接DATA节点
			DataClient client = createClient(host); 
			if (client == null) {
				Logger.error("DataPool.conduct, cannot find client:%s", host);
				break;
			}
			// 立即执行conduct检索
			trustor.send(client, phase);
		}
		
//		// 启动检索任务
//		trustor.launch();
		
		// 等待检索结果
		trustor.waiting();
		// 取回DATA分段结果集合
		byte[] data = trustor.data();
		if (data == null || data.length == 0) {
			Logger.warning("DataPool.conduct, cannot find data!");
			return null;
		}
		// 进入"AGGREGATE"阶段，必须保证有WORK收到数据后，按照已经定义的编号，有序进行保存数据
		return WorkPool.getInstance().conduct(conduct, data, 0, data.length);
	}

//	/**
//	 * @param conduct - 命令
//	 * @param ignoreChunkids - 被忽略的CHUNK标识
//	 * @return
//	 */
//	public byte[] conduct(Conduct conduct, long[] ignoreChunkids) {
//		conduct.setRunid(this.nextIdentity());
//				
//		// 启动初始化计算，分配各阶段任务参数的定义
//		conduct = (Conduct) this.initTask(conduct);
//		if (conduct == null) {
//			Logger.error("DataPool.conduct, init error!");
//			return null;
//		}
//		
//		FromObject from = conduct.getFrom();
//		if(from.countSelect() > 0) {
//			return conduct_select(conduct, ignoreChunkids);
//		} else {
//			return conduct_notselect(conduct);
//		}
//	}
	
//	private byte[] adc_select(ADC adc, long[] filteChunkIds) {
//		Select select = adc.getFromSelect();
//		Space space = select.getSpace();
//		
////		IndexModule module = null;
////		super.lockMulti();
////		try {
////			module = mapModule.get(space);
////		} catch (Throwable exp) {
////			Logger.fatal(exp);
////		} finally {
////			super.unlockMulti();
////		}
//		
//		IndexModule module = findModule(space);
//		if (module == null) {
//			Logger.fatal("DataPool.adc_select, cannot find index module for '%s'", space);
//			return null;
//		}
//		ChunkIdentitySet tokens = new ChunkIdentitySet();
//		int count = module.find(select.getCondition(), tokens);
//		if (count <= 0) {
//			Logger.warning("DataPool.adc_select, cannot find chunk identity");
//			return null;
//		}
//		
//		// 删除冗余的chunkid
//		if(filteChunkIds != null) {
//			tokens.remove(filteChunkIds);
//			if(tokens.isEmpty()) {
//				Logger.warning("DataPool.adc_select, null chunkid set");
//				return null;
//			}
//		}
//
//		// 按照chunkid,找到匹配的主机地址
//		Map<SiteHost, ChunkIdentitySet> map = new HashMap<SiteHost, ChunkIdentitySet>();
//		for(long chunkId : tokens.list()) {
//			SiteSet set = null;
//			super.lockMulti();
//			try {
//				set = mapChunk.get(chunkId);
//			} catch (Throwable exp) {
//				Logger.fatal(exp);
//			} finally {
//				super.unlockMulti();
//			}
//			if(set == null) {
//				Logger.error("DataPool.adc_select, cannot find chunk id [%d - %x]", chunkId, chunkId);
//				continue;
//			}
//			SiteHost host = set.next();
//			if(host == null) {
//				Logger.error("DataPool.adc_select, cannot find host %s", host);
//				continue;
//			}
//			// 分割chunkid
//			ChunkIdentitySet id_set = map.get(host);
//			if(id_set == null) {
//				id_set = new ChunkIdentitySet();
//				map.put(host, id_set);
//			}
//			id_set.add(chunkId);
//		}
//		
//		int fromsites = map.size();
//		if(fromsites == 0) {
//			Logger.warning("DataPool.adc_select, cannot find host!");
//			return null;
//		}
//		DataTrustor finder = new DataTrustor(fromsites);
//		
//		for(SiteHost host : map.keySet()) {
//			ChunkIdentitySet id_set = map.get(host);
//			// find data-client
//			DataClient client = findClient(host);
//			if(client == null) {
//				Logger.error("DataPool.adc_select, cannot find client:%s", host);
//				break;
//			}
//			Select clone_select = (Select) select.clone();
//			long[] chunkIds = id_set.toArray();
//			clone_select.setChunkids(chunkIds);
//			ADC clone_adc = (ADC)adc.clone();
//			clone_adc.setFromSelect(clone_select);
//			finder.add(client, clone_adc);
//		}
//
//		if(fromsites != finder.size()) {
//			Logger.error("DataPool.adc_select, not match, exit!");
//			finder.discontinue(true);
//			return null;
//		}
//
//		// 启动检索任务
//		finder.execute();
//		// 等待检索结果
//		finder.waiting();
//		// 取回DATA分段结果集合
//		byte[] data = finder.data();
//		if (data == null || data.length == 0) {
//			Logger.warning("DataPool.adc_select, cannot find data!");
//			return null;
//		}
//
//		// 解析 DCArea
//		ArrayList<DCArea> areas = new ArrayList<DCArea>();
//		for (int off = 8; off < data.length;) {
//			DCArea area = new DCArea();
//			int len = area.resolve(data, off, data.length - off);
//			off += len;
//			areas.add(area);
//		}
//		
//		// 必须保证有WORK收到数据后，按照已经定义的编号，有序进行保存数据
//		return WorkPool.getInstance().adc(adc, areas);
//	}
	
//	private void initChooser(InitTask task, Distribute distribute) {
//		InitChooser chooser = task.getChooser();
//
//		List<Space> list = new ArrayList<Space>();
//		for (Select select : distribute.getFrom().getSelects()) {
//			Space space = select.getSpace();
//			list.add(space);
//		}
//		// 找到Table, IndexModule, CodePointModule，写入配置
//		for (Space space : list) {
//			Table table = findTable(space);
//			if (table == null) continue;
//			chooser.addTable(table);
//
//			IndexModule module = findModule(space);
//			if (module == null) continue;
//
//			chooser.addIndexModule(module);
//
//			for (ColumnAttribute attribute : table.values()) {
//				if (attribute.isKey() && attribute.isWord()) {
//					Docket deck = new Docket(space, attribute.getColumnId());
//					CodePointModule cpm = CodePointPool.getInstance()
//							.findModule(deck);
//					if (cpm == null)
//						continue;
//					chooser.addCodePointModule(cpm);
//				}
//			}
//		}
//
//	}
	


//	private byte[] conduct_select(Conduct conduct, long[] ignoreChunkids) {
//		
//		//2. 执行"diffuse"阶段任务
//		int allSites = 0;
//		DataTrustor trustor = new DataTrustor();
//
//		FromObject from = conduct.getFrom();
//		for(Select select : from.getSelects()) {
//			Space space = select.getSpace();
//			IndexModule module = findModule(space);
//			if (module == null) {
//				Logger.fatal("DataPool.conduct_select, cannot find index module for '%s'", space);
//				return null;
//			}
//			ChunkSet tokens = new ChunkSet();
//			int count = module.find(select.getCondition(), tokens);
//			if (count <= 0) {
//				Logger.warning("DataPool.conduct_select, cannot find chunk identity");
//				return null;
//			}
//
//			// 删除冗余的chunkid
//			if(ignoreChunkids != null) {
//				tokens.remove(ignoreChunkids);
//				if(tokens.isEmpty()) {
//					Logger.warning("DataPool.conduct_select, null chunkid set");
//					return null;
//				}
//			}
//
//			// 按照chunkid，找到匹配的主机地址
//			Map<SiteHost, ChunkSet> map = new HashMap<SiteHost, ChunkSet>();
//			for(long chunkid : tokens.list()) {
//				SiteSet set = this.findDataNodes(chunkid);
//				if(set == null) {
//					Logger.error("DataPool.conduct_select, cannot find chunk id [%d - %x]", chunkid, chunkid);
//					continue;
//				}
//				SiteHost host = set.next();
//				if(host == null) {
//					Logger.error("DataPool.conduct_select, cannot find host %s", host);
//					continue;
//				}
//				// 分割chunkid
//				ChunkSet id_set = map.get(host);
//				if(id_set == null) {
//					id_set = new ChunkSet();
//					map.put(host, id_set);
//				}
//				id_set.add(chunkid);
//			}
//
//			if(map.isEmpty()) {
//				Logger.warning("DataPool.conduct_select, cannot find host!");
//				return null;
//			}
//			allSites += map.size();
//
//			for(SiteHost host : map.keySet()) {
//				ChunkSet id_set = map.get(host);
//				// find data-client
//				DataClient client = findClient(host);
//				if(client == null) {
//					Logger.error("DataPool.conduct_select, cannot find client:%s", host);
//					break;
//				}
//				Select clone_select = (Select) select.clone();
//				long[] chunkIds = id_set.toArray();
//				clone_select.setChunkids(chunkIds);
//
//				// 克隆ADC的FROM单元只能有一个"SQL SELECT"
//				Conduct clone_conduct = (Conduct)conduct.clone();
//				clone_conduct.getFrom().clearSelects();
//				clone_conduct.getFrom().addSelect(clone_select);
//				trustor.add(client, clone_conduct);
//			}
//		}
//		
//		if(allSites != trustor.size()) {
//			Logger.error("DataPool.conduct_select, not match, exit!");
//			trustor.disconnect(true);
//			return null;
//		}
//
//		// 启动检索任务
//		trustor.launch();
//		// 等待检索结果
//		trustor.waiting();
//		// 取回DATA分段结果集合
//		byte[] data = trustor.data();
//		if (data == null || data.length == 0) {
//			Logger.warning("DataPool.conduct_select, cannot find data!");
//			return null;
//		}
//
////		// 解析 DCArea
////		ArrayList<DCArea> areas = new ArrayList<DCArea>();
////		// 跨过"头标记"域
////		int seek = ReturnTag.length();
////		while (seek < data.length) {
////			DCArea area = new DCArea();
////			int size = area.resolve(data, seek, data.length - seek);
////			seek += size;
////			areas.add(area);
////		}
//		
//		// 进入"AGGREGATE"阶段，必须保证有WORK收到数据后，按照已经定义的编号，有序进行保存数据
//		return WorkPool.getInstance().conduct(conduct, data, 0, data.length);
//	}

//	/**
//	 * @param conduct
//	 * @return
//	 */
//	private byte[] conduct_notselect(Conduct conduct) {
//		// 启动前初始化
//		InitObject init = conduct.getInit();
//		if (init != null) {
//			Naming naming = init.getNaming();
//			InitTask task = InitTaskPool.getInstance().find(naming);
//			if (task == null) {
//				Logger.error("DataPool.conduct_notselect, cannot find:%s", naming);
//				return null;
//			}
//			// 暂时不定义参数，由接口完成初始化
////			InitChooser chooser = this.getChooser(conduct);
////			conduct = task.init(conduct, chooser);
//			
////			this.initChooser(task, conduct);
//			conduct = (Conduct) task.init(conduct);
//		}
//		
//		FromObject from = conduct.getFrom();
//		//1. find naming host, and check host number
//		int from_sites = from.getSites(); // adc.getFromSites();
//		if(from_sites < 1) from_sites = 1;
//
//		// 根据命名，找到对应的DATA节点地址集合
//		Naming naming = from.getTaskNaming();
//		SiteSet set = this.findDataNodes(naming);
//		if (set == null) {
//			Logger.error("DataPool.conduct_notselect, cannot find naming:%s", naming);
//			return null;
//		} else if (set.size() < from_sites) {
//			Logger.error("DataPool.conduct_notselect, host missing!");
//			return null;
//		}
//		
//		ArrayList<SiteHost> hosts = new ArrayList<SiteHost>();
//		for (int i = 0; i < from_sites; i++) {
//			hosts.add(set.next());
//		}
//				
//		//2. 启动代理服务
//		DataTrustor trustor = new DataTrustor(from_sites);
//		int siteIndex = 1;
//		for (SiteHost host : hosts) {
//			DataClient client = findClient(host);
//			if (client == null) {
//				Logger.error("DataPool.conduct_notselect, cannot connect %s", host);
//				break;
//			}
//			// 克隆Conduct
//			Conduct clone_conduct = (Conduct) conduct.clone();
//			// 为第个克隆ADC分配序列号
//			clone_conduct.getFrom().setIndex(siteIndex++);
//			// 加入代理队列
//			trustor.add(client, clone_conduct);
//		}
//		
//		if(from_sites != trustor.size()) {
//			Logger.error("DataPool.conduct_notselect, not match, exit!");
//			trustor.disconnect(true);
//			return null;
//		}
//
//		// 启动"diffuse"阶段的任务
//		trustor.launch();
//		// 等待线程结束
//		trustor.waiting();
//		// 取出数据
//		byte[] data = trustor.data();
//		if (data == null || data.length == 0) {
//			Logger.warning("DataPool.conduct_notselect, cannot find data!");
//			return null;
//		}
//		
//		Logger.debug("DataPool.conduct_notselect, diffuse conduct data size:%d", data.length);
//		
////		// 解析"DCArea"，跨过"items"域 (8字节)
////		List<DCArea> array = new ArrayList<DCArea>();
////		// 跨过"头标记" 字段
////		int seek = ReturnTag.length();
////		while (seek < data.length) {
////			DCArea area = new DCArea();
////			int size = area.resolve(data, seek, data.length - seek);
////			seek += size;
////			array.add(area);
////		}
//		
//		// "aggregate"任务阶段
//		return WorkPool.getInstance().conduct(conduct, data, 0, data.length);
//	}
	
	
//	/**
//	 * @param adc
//	 * @return
//	 */
//	private byte[] adc_notselect(ADC adc) {
//		//1. find naming host, and check host number
//		int from_sites = adc.getFromSites();
//		if(from_sites < 1) from_sites = 1;
//
//		SiteSet set = null;
//		super.lockMulti();
//		try {
//			set = mapNaming.get( new Naming(adc.getFromNaming()) );
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//		if (set == null) {
//			Logger.error("DataPool.adc_notselect, cannot find naming:%s", adc.getFromNaming());
//			return null;
//		} else if (set.size() < from_sites) {
//			Logger.error("DataPool.adc_notselect, host missing!");
//			return null;
//		}
//		
//		ArrayList<SiteHost> hosts = new ArrayList<SiteHost>();
//		for (int i = 0; i < from_sites; i++) {
//			hosts.add(set.next());
//		}
//		
//		//2. find data-client, clone "ADC", and set a index(from 1)\
//		adc.defineFromSites(from_sites);
//		DataDelegate finder = new DataDelegate(from_sites);
//
//		int siteIndex = 1;
//		for (SiteHost host : hosts) {
//			DataClient client = findClient(host);
//			if (client == null) {
//				Logger.error("DataPool.adc_notselect, cannot connect %s", host);
//				break;
//			}
//			ADC clone_adc = (ADC) adc.clone();
//			clone_adc.defineFromIndex(siteIndex++);
//			finder.add(client, clone_adc);
//		}
//		
//		if(from_sites != finder.size()) {
//			Logger.error("DataPool.adc_notselect, not match, exit!");
//			finder.discontinue(true);
//			return null;
//		}
//
//		// "diffuse" job
//		finder.execute();
//		// wait job
//		finder.waiting();
//		// dc result
//		byte[] data = finder.data();
//		if(data == null || data.length ==0) {
//			Logger.warning("DataPool.adc_notselect, cannot find data!");
//			return null;
//		}
//		
//		Logger.debug("DataPool.adc_notselect, diffuse adc data size:%d", data.length);
//		
//		// resolve "DCArea" set, skip "items" value(8 bits)
//		List<DCArea> array = new ArrayList<DCArea>();
//		for(int off = 8; off < data.length; ) {
//			DCArea area = new DCArea();
//			int len = area.resolve(data, off, data.length - off);
//			off += len;
//			array.add(area);
//		}
//		
//		// "aggregate" job
//		return WorkPool.getInstance().adc(adc, array);
//	}

//	private void selectOrderBy(Select select) {
//		Space space = select.getSpace();
//		Table table = mapTable.get(space);
//		
//		short columnId = select.getOrderBy().getColumnId();
//		IndexModule module = findModule(space);
//		
//		IndexChart chart = module.find(columnId);
//		
//		String naming = "system_orderby_diffuse";
//		// 查找命名对象
//		List<SiteHost> works = WorkPool.getInstance().find(naming);
//		
//		ColumnAttribute attribute =	table.find(columnId);
//		ColumnSector sector = null;
//		
//		// 如果是字符串，从CodePointPool取分片记录
//		if (attribute.isWord()) {
//			Docket deck = new Docket(space, attribute.getColumnId());
//			IntegerZone[] zones = CodePointPool.getInstance().find(deck);
//			WCharBalancer balancer = new WCharBalancer();
//			for (int i = 0; i < zones.length; i++) {
//				balancer.add(zones[i]);
//			}
//			// 根据WORK主机数量，进行分片
//			sector = balancer.balance(works.size());
//		} else if (attribute.isShort()) {
//			// 1. 从集合中取出全部分片范围，同时过滤最大最小的分片
//			IndexZone[] zones = chart.find(true, java.lang.Short.MIN_VALUE, java.lang.Short.MAX_VALUE);
//			// 2. 分片保存到平衡器
//			ShortBalancer balancer = new ShortBalancer();
//			for (int i = 0; i < zones.length; i++) {
//				balancer.add(zones[i]);
//			}
//			// 3. 根据WORK节点数量，产生一个分片集合
//			sector = balancer.balance(works.size());
//		} else if (attribute.isInteger()) {
//			
//		} else if(attribute.isDate()) {
//			
//		}
//		
//		// WORK主机数量和地址写入to段
//		ToObject to = new ToObject("SYSTEM_SELECT_ORDER_TO");
//		to.setSites( works.size() );
//		for (SiteHost host : works) {
//			to.addSite(host);
//		}
//		
//		// 分片数据写入from段
//		FromObject from = new FromObject("SYSTEM_SELECT_ORDER_FROM");
//		from.addSelect(select);
////		CRaw raw = new CRaw("SYSTEM_SELECT_ORDERBY", sector.build());
////		from.addValue(raw);
//		
//		from.addSector("SYSTEM_SELECT_ORDERBY", sector);
//		
//		Conduct conduct = new Conduct();
//		conduct.setTo(to);
//		conduct.setFrom(from);
//		
//		// 执行conduct操作，返回结果集合
//		this.conduct(conduct);
//		
//		
//		// 收到分片结果，通知WORK主机，到指定的地址去接收信息
//		
//		
//	}

//	/**
//	 * update all datasite
//	 */
//	private void refreshSite() {		
//		HomeClient client = super.bring(false);
//		if(client == null) {
//			Logger.error("DataPool.refreshSite, cannot connect home-site:%s", home);
//			return;
//		}
//		
//		boolean error = true;
//		DataSite[] sites = null;
//		try {
//			sites = (DataSite[]) client.batchDataSite();
//			error = false;
//		} catch (VisitException exp) {
//			Logger.error(exp);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		}
//		this.complete(client);
//
//		if (error) {
//			Logger.warning("DataPool.refreshSite, visit error!");
//			return;
//		}
//		
//		Logger.debug("DataPool.refreshSite, data site size:%d", (sites == null ? -1 : sites.length));
//		
//		// check space
//		Map<Naming, SiteSet> map_naming = new TreeMap<Naming, SiteSet>();
//		Map<Space, SiteSet> map_space = new TreeMap<Space, SiteSet>();
//		Map<Long, SiteSet> map_chunk = new TreeMap<Long, SiteSet>();
//		Map<Space, IndexModule> map_index = new TreeMap<Space, IndexModule>();
//		List<SiteHost> allsite = new ArrayList<SiteHost>();
//		
//		for (int i = 0; sites != null && i < sites.length; i++) {
//			DataHost host = new DataHost(sites[i].getHost(), sites[i].getRank());
//			allsite.add(host);
//			// 保存全部"diffuse"命名
//			for (Naming naming : sites[i].listNaming()) {
//				SiteSet set = map_naming.get(naming);
//				if (set == null) {
//					set = new SiteSet();
//					map_naming.put(naming, set);
//				}
//				set.add(host);
//			}
//
//			IndexSchema schema = sites[i].getIndexSchema();
//			for(Space space : schema.keySet()) {
//				if (!callInstance.containsSpace(space)) {
//					continue;
//				}
//				
//				IndexTable indexTable = schema.find(space);
//				// save space -> host address
//				SiteSet set = map_space.get(space);
//				if (set == null) {
//					set = new SiteSet();
//					map_space.put(space, set);
//				}
//				set.add(host);
//				// save module
//				IndexModule module = map_index.get(space);
//				if(module == null) {
//					module = new IndexModule(space);
//					map_index.put(space, module);
//				}
//				for(long chunkId : indexTable.keys()) {
//					ChunkAttribute sheet = indexTable.find(chunkId);
//					for (short columnId : sheet.keys()) {
//						IndexRange index = sheet.find(columnId);
//						module.add(host, index);
//					}
//					// save data site
//					set = map_chunk.get(chunkId);
//					if (set == null) {
//						set = new SiteSet();
//						map_chunk.put(chunkId, set);
//					}
//					set.add(host);
//				}
//			}
//		}
//
//		// update all record
//		super.lockSingle();
//		try {
//			mapNaming.clear();
//			mapChunk.clear();
//			mapModule.clear();
//			mapSpace.clear();
//			mapPrime.clear();
//
//			mapNaming.putAll(map_naming);
//			mapSpace.putAll(map_space);
//			mapChunk.putAll(map_chunk);
//			mapModule.putAll(map_index);
//			
//			ArrayList<SiteHost> excludes = new ArrayList<SiteHost>();
//			for (SiteHost host : mapClient.keySet()) {
//				if (!allsite.contains(host)) excludes.add(host);
//			}
//
//			for (SiteHost host : excludes) {
//				ClientSet set = mapClient.remove(host);
//				int size = set.size();
//				for (int i = 0; i < size; i++) {
//					DataClient ds = (DataClient) set.get(i);
//					ds.stop();
//				}
//			}
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//		
////		CallSite site = super.callInstance.getLocal();
////		site.clearDiffuseNaming();
////		for (Naming naming : map_naming.keySet()) {
////			Logger.debug("DataPool.refreshSite, add naming:%s", naming);
////			site.addDiffuseNaming(naming.toString());
////		}
//		
//		CallSite site = super.callInstance.getLocal();
//		site.updateFromNamings(map_naming.keySet());
//		
//		super.callInstance.setOperate(BasicLauncher.RELOGIN);
//	}
//
//	/**
//	 * 停止这个空间下的所有连接
//	 * @param space
//	 */
//	public int stopSpace(Space space) {
//		int count = 0;
//		super.lockSingle();
//		try {
//			mapTable.remove(space);
//			mapPrime.remove(space);
//			
//			SiteSet set = mapSpace.remove(space);
//			if (set == null) return -1;
//			IndexModule module = mapModule.remove(space);
//			if(module == null) return -1;
//			
//			for(SiteHost host : set.list()) {
//				List<Long> list = module.delete(host);
//				if(list == null || list.isEmpty()) continue;
//				for(long chunkId : list) {
//					SiteSet sub = mapChunk.get(chunkId);
//					if(sub != null) {
//						if(sub.remove(host)) count++;
//					}
//					if(sub == null || sub.isEmpty()) {
//						mapChunk.remove(chunkId);
//					}
//				}
//			}
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//		
//		Logger.info("DataPool.stopSpace, remove chunkid count:%d", count);
//		return count;
//	}
//	
//	private void check() {
//		if (refresh) {
//			refresh = false;
//			this.refreshSite();
//		}
//	}
	
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
		Logger.info("DataPool.process, into...");

		this.delay(10000);
//		this.refresh = true;
		
		while(!super.isInterrupted()) {
//			this.check();
			this.delay(5000);
		}
		Logger.info("DataPool.process, exit");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
//		stopClients();
	}

}