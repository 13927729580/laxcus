package com.lexst.pool.site;

import java.util.*;

import com.lexst.algorithm.choose.*;
import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.remote.client.home.*;
import com.lexst.site.data.*;
import com.lexst.sql.charset.codepoint.*;
import com.lexst.sql.chunk.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.index.range.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;
import com.lexst.visit.*;

/**
 * diffuse阶段所有配置的数据存储池。<br>
 * 工作范围: <br>
 * <1> 接受HOME节点通知，从HOME节点上提取全部DATA节点数据，在本地进行筛选。<br>
 * <2> 维护这些存储配置。<br>
 * 
 */
public class FromPool extends JobPool implements FromChooser {
	
	private static FromPool selfHandle = new FromPool();
	
	/** FROM数据池监听器，由宿主节点实现并且赋值 **/
	private FromListener listener;

	/** 表名 -> 表配置 */
	private Map<Space, Table> mapTable = new TreeMap<Space, Table>();

	/** 数据表+数据表列 -> 代码位集合 **/
	private Map<Docket, CodeIndexModule> mapCodeIndex = new TreeMap<Docket, CodeIndexModule>();

	/** HOME节点通知，更新当前的全部DATA节点记录 **/
	private boolean refresh;

	/** 以下数据将在更新节点的全部替换 **/
	/** 表名 -> 索引集合 */
	private Map<Space, IndexModule> mapModule = new TreeMap<Space, IndexModule>();

	/** 数据块标识号 -> 节点地址集合 */
	private Map<Long, SiteSet> mapChunk = new TreeMap<Long, SiteSet>();

	/** 表名 -> 节点地址集合 */
	private Map<Space, SiteSet> mapSpace = new HashMap<Space, SiteSet>(16);

	/** diffuse任务命名 -> 节点地址集合 **/
	private Map<Naming, SiteSet> mapNaming = new TreeMap<Naming, SiteSet>();

	/** 任务命名+数据表空间 -> 节点地址集合 */
	private Map<Anchor, SiteSet> mapAnchor = new TreeMap<Anchor, SiteSet>();

	/** 表名 -> DATA主节点地址 */
	private Map<Space, SiteSet> mapPrimes = new HashMap<Space, SiteSet>();



	/**
	 * default
	 */
	private FromPool() {
		super();
	}
	
	/**
	 * 返回静态句柄
	 * @return
	 */
	public static FromPool getInstance() {
		return FromPool.selfHandle;
	}
	
	/**
	 * 设置FROM监听器
	 * @param s
	 */
	public void setFromListener(FromListener s) {
		this.listener = s;
	}
	
	/**
	 * 返回FROM监听器
	 * @return
	 */
	public FromListener getFromListener() {
		return this.listener;
	}
	
	/**
	 * 更新状态，通知线程从HOME节点取新数据
	 */
	public void refresh() {
		this.refresh = true;
		wakeup();
	}
	
	@Override
	public List<Naming> getNamings() {
		List<Naming> array = new ArrayList<Naming>();
		super.lockMulti();
		try {
			array.addAll(mapNaming.keySet());
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockMulti();
		}
		return array;
	}

	@Override
	public List<Space> getSpaces() {
		List<Space> array = new ArrayList<Space>();
		super.lockMulti();
		try {
			array.addAll(mapTable.keySet());
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockMulti();
		}
		return array;
	}
	
	/*
	 * 外部(通常是各节点的Launcher)向存储集增加一个数据库表
	 * @see com.lexst.algorithm.choose.FromChooser#addTable(com.lexst.sql.schema.Table)
	 */
	@Override
	public boolean addTable(Table table) {
		boolean success = false;
		super.lockSingle();
		try {
			Space space = (Space) table.getSpace();
			success = (mapTable.put(space, table) == null);
		} catch (Throwable e) {
			Logger.fatal(e);
		} finally {
			super.unlockSingle();
		}
		
		Logger.note(success, "FromPool.addTable, add %s", table.getSpace());
		return success;
	}

	/*
	 * 删除数据库表配置
	 * @see com.lexst.algorithm.choose.FromChooser#removeTable(com.lexst.sql.schema.Space)
	 */
	@Override
	public boolean removeTable(Space space) {
		boolean success = false;
		super.lockSingle();
		try {
			success = (mapTable.remove(space) != null);
		} catch (Throwable e) {
			Logger.fatal(e);
		} finally {
			super.unlockSingle();
		}
		
		Logger.note(success, "FromPool.removeTable, remove %s", space);
		return success;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.algorithm.choose.FromChooser#findTable(com.lexst.sql.schema.Space)
	 */
	@Override
	public Table findTable(Space space) {
		// 从内存里查找表配置
		Table table = null;
		super.lockMulti();
		try {
			table = mapTable.get(space);
		} catch(Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		if(table != null) return table;
		
		Logger.info("FromPool.findTable, cannot find '%s', to home...", space);

		// 启动HOME连接查找表配置
		HomeClient client = super.bring(false);
		if(client == null) return null;
		try {
			table = client.findTable(space);
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable e) {
			Logger.fatal(e);
		}
		this.complete(client);
		
		Logger.note(table != null, "FromPool.findTable, find %s", space);
		if (table == null) return null;

		// 数据库表配置保存到内存中
		super.lockSingle();
		try {
			mapTable.put(space, table);
		} catch (Throwable e) {
			Logger.fatal(e);
		} finally {
			super.unlockSingle();
		}		

		return table;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.algorithm.choose.FromChooser#findIndexModule(com.lexst.sql.schema.Space)
	 */
	@Override
	public IndexModule findIndexModule(Space space) {
		super.lockMulti();
		try {
			return this.mapModule.get(space);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return null;
	}

	@Override
	public CodeIndexModule findCodeIndexModule(Docket docket) {
		super.lockMulti();
		try {
			return mapCodeIndex.get(docket);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return null;
	}

	@Override
	public SiteSet findFromSites(Naming naming) {
		super.lockMulti();
		try {
			return mapNaming.get(naming);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return null;
	}

	@Override
	public SiteSet findFromSites(Naming naming, Space space) {
		Anchor anchor = new Anchor(naming, space);
		super.lockMulti();
		try {
			return mapAnchor.get(anchor);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return null;
	}

	@Override
	public SiteSet findFromSites(long chunkid) {
		super.lockMulti();
		try {
			return mapChunk.get(chunkid);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return null;
	}
	
	/**
	 * 查找某一数据库表的主节点
	 * @param space
	 * @return
	 */
	@Override
	public SiteSet findPrime(Space space) {
		super.lockMulti();
		try {
			return mapPrimes.get(space);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		long interval = 20 * 60 * 1000;
		long endTime = System.currentTimeMillis();
		
		this.delay(10000);
		this.refresh = true;

		while(!super.isInterrupted()) {
			// 收到通知,更新DATA节点记录
			if (refresh) {
				refresh = false;
				this.refreshData();
			}
			// 更新代码位(20分钟更新一次)
			if(System.currentTimeMillis() >= endTime) {
				endTime = System.currentTimeMillis() + interval;
				this.refreshCodeIndex();
			}
			// 延时
			this.delay(5000);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		Logger.info("FromPool.finish, stop all!");
	}

	/**
	 * 更新全部DATA节点信息
	 */
	private void refreshData() {
		// 连接HOME节点
		HomeClient client = super.bring(false);
		if(client == null) {
			Logger.error("FromPool.refreshSite, cannot connect home-site:%s", getHome());
			return;
		}
		
		// 批量提取全部DATA节点记录
		boolean error = true;
		DataSite[] sites = null;
		try {
			sites = (DataSite[]) client.batchDataSite();
			error = false;
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable t) {
			Logger.fatal(t);
		}
		this.complete(client);

		if (error) {
			Logger.warning("FromPool.refreshSite, visit error!");
			return;
		}
		
		Logger.debug("FromPool.refreshSite, data site size:%d", (sites == null ? 0 : sites.length));
		
		// 检查所有参数，提取对应的数据
		Map<Naming, SiteSet> map_naming = new TreeMap<Naming, SiteSet>();
		Map<Space, SiteSet> map_space = new TreeMap<Space, SiteSet>();
		Map<Anchor, SiteSet> map_anchor = new TreeMap<Anchor, SiteSet>();
		Map<Long, SiteSet> map_chunk = new TreeMap<Long, SiteSet>();
		Map<Space, SiteSet> map_prime = new TreeMap<Space, SiteSet>();
		Map<Space, IndexModule> map_index = new TreeMap<Space, IndexModule>();
		
		for (int i = 0; sites != null && i < sites.length; i++) {
			SiteHost host = new SiteHost(sites[i].getHost());
			
			// 保存全部"diffuse"命名
			for (Naming naming : sites[i].listNaming()) {
				SiteSet set = map_naming.get(naming);
				if (set == null) {
					set = new SiteSet();
					map_naming.put((Naming) naming.clone(), set);
				}
				set.add(host);
			}

			// 索引记录
			IndexSchema schema = sites[i].getIndexSchema();
			for (Space space : schema.keySet()) {
				// 保存基于主节点的数据库表和主机地址集合
				if (sites[i].isPrime()) {
					SiteSet set = map_prime.get(space);
					if (set == null) {
						set = new SiteSet();
						map_prime.put((Space) space.clone(), set);
					}
					set.add(host);
				}

				// 保存全部 diffuse命名 + 数据库表名 配置，用于字符类型的代码位计算
				for (Naming naming : sites[i].listNaming()) {
					Anchor anchor = new Anchor(naming, space);
					SiteSet set = map_anchor.get(anchor);
					if (set == null) {
						set = new SiteSet();
						map_anchor.put((Anchor) anchor.clone(), set);
					}
					set.add(host);
				}
				
				// 保存每个表的索引模块
				IndexModule module = map_index.get(space);
				if(module == null) {
					module = new IndexModule(space);
					map_index.put((Space) space.clone(), module);
				}
				
				// 保存数据块和主机地址集合
				IndexTable indexTable = schema.find(space);
				for(long chunkid : indexTable.keys()) {
					ChunkAttribute attribute = indexTable.find(chunkid);
					for (short columnId : attribute.keys()) {
						IndexRange range = attribute.find(columnId);
						module.add(host, range);
					}
					// 保存数据块标识和主机地址集合
					SiteSet set = map_chunk.get(chunkid);
					if (set == null) {
						set = new SiteSet();
						map_chunk.put(chunkid, set);
					}
					set.add(host);
				}
				
				// 保存数据库表和主机的地址集合
				SiteSet set = map_space.get(space);
				if (set == null) {
					set = new SiteSet();
					map_space.put((Space) space.clone(), set);
				}
				set.add(host);
			}
		}

		// 减少内存占用
		for(SiteSet set : map_naming.values()) set.trim();
		for(SiteSet set : map_anchor.values()) set.trim();
		for(SiteSet set : map_space.values()) set.trim();
		for(SiteSet set : map_chunk.values()) set.trim();
		for(SiteSet set : map_prime.values()) set.trim();
		
		// 显示更新日志
		Logger.debug("FromPool.refreshSite, diffuse naming size is: %d", map_naming.size());
		Logger.debug("FromPool.refreshSite, space size is: %d", map_space.size());
		Logger.debug("FromPool.refreshSite, anchor size is: %d", map_anchor.size());
		Logger.debug("FromPool.refreshSite, chunk size is: %d", map_chunk.size());
		Logger.debug("FromPool.refreshSite, prime site size is: %d", map_prime.size());
		Logger.debug("FromPool.refreshSite, space index size is: %d", map_index.size());

		// 锁定，更新全部记录
		super.lockSingle();
		try {
			mapNaming.clear();
			mapAnchor.clear();
			mapChunk.clear();
			mapModule.clear();
			mapSpace.clear();
			mapPrimes.clear();

			mapNaming.putAll(map_naming);
			mapAnchor.putAll(map_anchor);
			mapSpace.putAll(map_space);
			mapChunk.putAll(map_chunk);
			mapModule.putAll(map_index);		
			mapPrimes.putAll(map_prime);
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockSingle();
		}
		
		// 通知宿主已经更新记录
		this.listener.updateDataRecord();
	}
	
	/**
	 * 更新代码位集合
	 */
	private void refreshCodeIndex() {
		List<Docket> array = new ArrayList<Docket>();
		List<Space> list = this.getSpaces();
		for (Space space : list) {
			Table table = this.findTable(space);
			for (ColumnAttribute attribute : table.values()) {
				if (attribute.isWord()) {
					array.add(new Docket(space, attribute.getColumnId()));
					break;
				}
			}
		}
		
		if(array.isEmpty()) return;
		
		Map<Docket, CodeIndexModule> set = new TreeMap<Docket, CodeIndexModule>();
		HomeClient client = super.bring();
		if(client == null) return;
		
		for(Docket docket : array) {
			int[] codePoints = null;
			try {
				codePoints = client.findCodePoints(docket.getSchema(), docket.getTable(), docket.getColumnId());
			} catch (VisitException exp) {
				Logger.error(exp);
				break;
			}

			// 出错
			if(codePoints == null || codePoints.length % 3 !=0) {
				continue;
			}
			
			CodeIndexModule module = new CodeIndexModule(docket);
			set.put(docket, module);
			// 保存代码位映射集
			for (int i = 0; i < codePoints.length; i += 3) {
				int begin = codePoints[i];
				int end = codePoints[i + 1];
				int count = codePoints[i + 2];
				module.add(begin, end, count);
			}
		}
		// 关闭网络连接
		super.complete(client);

		// 更新全部结果
		super.lockSingle();
		try {
			this.mapCodeIndex.putAll(set);
		} catch(Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
	}

}