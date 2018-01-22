/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.init.select;

import java.util.*;

import com.lexst.algorithm.init.*;
import com.lexst.log.client.*;
import com.lexst.sql.*;
import com.lexst.sql.charset.codepoint.*;
import com.lexst.sql.chunk.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.value.*;
import com.lexst.sql.index.balance.*;
import com.lexst.sql.index.chart.*;
import com.lexst.sql.index.section.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.select.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;

/**
 * 系统级初始化命名
 *
 */
public class SelectInitTask extends InitTask {
	
	private final static String SELECT_DIFFUSE = "LAXCUS_SYSTEM_SELECT_DIFFUSE";
	private final static String SELECT_GROUPBY = "LAXCUS_SYSTEM_AGGREGATE_SELECT_GROUPBY";
	private final static String SELECT_ORDERBY = "LAXCUS_SYSTEM_AGGREGATE_SELECT_ORDERBY";

	/**
	 * default
	 */
	public SelectInitTask() {
		super();
	}
	
	private IndexBalancer doBalancer(ColumnAttribute attribute) {
		switch(attribute.getType()) {
		case Type.RAW:
			return new RawBalancer();
		case Type.CHAR:
			return new CharBalancer();
		case Type.SCHAR:
			return new SCharBalancer();
		case Type.WCHAR:
			return new WCharBalancer();
		case Type.SHORT:
			return new ShortBalancer();
		case Type.INTEGER:
			return new IntegerBalancer();
		case Type.LONG:
			return new LongBalancer();
		case Type.FLOAT:
			return new FloatBalancer();
		case Type.DOUBLE:
			return new DoubleBalancer();
		case Type.DATE:
			return new DateBalancer();
		case Type.TIME:
			return new TimeBalancer();
		case Type.TIMESTAMP:
			return new TimestampBalancer();
		}
		return null;
	}
	
	private ColumnSector doSector(int sites, Space space, short columnId) {
		Table table = super.getFromChooser().findTable(space);
		ColumnAttribute attribute =	table.find(columnId);
		
		// 如果属性是字符按照代码位分片，否则按照索引值分片。
		IndexZone[] zones = null;
		if(attribute.isWord()) {
			Docket docket = new Docket(space, columnId);
			CodeIndexModule codeModule = getFromChooser().findCodeIndexModule(docket);
			// 如果没有找到，默认UTF16代码位：0x0 - 0xFFFF
			if (codeModule == null) {
				// 最大代码位分布
				zones = new IntegerZone[] { new IntegerZone(0, 0xFFFF, 1) };
			} else {
				// 实际有效代码位分布
				zones = codeModule.array();
			}
			
//			// 平衡分配
//			WCharBalancer balancer = new WCharBalancer();
//			for (int i = 0; i < zones.length; i++) {
//				balancer.add(zones[i]);
//			}
//			sector = balancer.balance( sites );

		} else {
			IndexModule indexModule = super.getFromChooser().findIndexModule(space);
			IndexChart chart = indexModule.find(columnId);
			//  从集合中取出全部分片范围，同时过滤最大最小的分片
			zones = chart.choice(true);
			
//			IndexZone[] zones = chart.find(true, java.lang.Short.MIN_VALUE, java.lang.Short.MAX_VALUE);
//			zones = chart.find(true, java.lang.Short.MIN_VALUE, java.lang.Short.MAX_VALUE);
//			// 2. 分片保存到平衡器
//			ShortBalancer balancer = new ShortBalancer();
//			for (int i = 0; i < zones.length; i++) {
//				balancer.add(zones[i]);
//			}
//			// 3. 根据WORK节点数量，产生一个分片集合
//			sector = balancer.balance( sites );
		}
		
		// 根据属性生成一个分片平衡器，并且存入分片记录
		IndexBalancer balancer = doBalancer(attribute);
		for(int i = 0; i < zones.length; i++) {
			balancer.add(zones[i]);
		}
		// 根据WORK节点数量生成分片集(按照权重尽可能平均分配)
		ColumnSector sector = balancer.balance(sites);
		
		return sector;
	}
	
	private FromOutputObject initFrom(Select select) {
		Space space = select.getSpace();
		// 找到Table、IndexModule、CodeIndexModule，写入配置
		Table table = super.getFromChooser().findTable(space);
		if (table == null) {
			Logger.error("SelectInitTask.init, cannot find %s", space);
			return null;
		}
		
		// 查找索引分区
		IndexModule indexModule = super.getFromChooser().findIndexModule(space);
		if (indexModule == null) {
			Logger.error("SelectInitTask.init, cannot find module:%s", space);
			return null;
		}
		
		// 根据WHERE语句，检索匹配的chunkid
		ChunkSet tokens = new ChunkSet();
		int count = indexModule.find(select.getCondition(), tokens);
		if (count < 0) {
			Logger.warning("SelectInitTask.initFrom, cannot find match chunk");
			return null;
		}
		
		// 根据chunkid，查找匹配的主机地址
		HashMap<SiteHost, ChunkSet> froms = new HashMap<SiteHost, ChunkSet>();
		for(long chunkid : tokens.list()) {
			SiteSet hosts = super.getFromChooser().findFromSites(chunkid);
			for (SiteHost host : hosts.list()) { 
				ChunkSet set = froms.get(host);
				if (set == null) {
					set = new ChunkSet();
					froms.put(host, set);
				}
				set.add(chunkid);
			}
			
//			SiteSet set = this.findDataNodes(chunkId);
//			if(set == null) {
//				Logger.error("DataPool.select, cannot find chunk id [%d - %x]", chunkId, chunkId);
//				continue;
//			}
//			SiteHost host = set.next();
//			if(host == null) {
//				Logger.error("DataPool.select, cannot find host %s", host);
//				continue;
//			}
//			// 分片
//			ChunkSet idset = map.get(host);
//			if(idset == null) {
//				idset = new ChunkSet();
//				map.put(host, idset);
//			}
//			idset.add(chunkId);
		}

		// 确定被分片的列. GROUPBY优先，ORDERBY其次
		String tonaming = null;
		short columnId = 0;
		if(select.getGroupBy() != null) {
			columnId = select.getGroupBy().listGroupIds()[0];
			tonaming = SelectInitTask.SELECT_GROUPBY;
		} else if(select.getOrderBy() != null) {
			columnId = select.getOrderBy().getColumnId();
			tonaming = SelectInitTask.SELECT_ORDERBY;
		}
		// 根据 aggregate阶段主机数确定最大分片数量(分片原则:可多不可少.这样实际命名主机不足时有缩小余地.而分片少则不能放大)
		SiteSet slaveSites = getToChooser().findToSites(new Naming(tonaming));
		int sites = slaveSites.size();
		ColumnSector sector = doSector(sites, space, columnId);
		
//		ColumnAttribute attribute =	table.find(columnId);
//		ColumnSector sector = null;
//		
//		if (attribute.isWord()) {
//			Docket docket = new Docket(space, columnId);
//			CodeIndexModule codeModule = super.getFromChooser().findCodeIndexModule(docket);
//			IntegerZone[] zones = null;
//			if (codeModule == null) {
//				// 最大代码位分布
//				zones = new IntegerZone[] { new IntegerZone(0, 0xFFFF, 1) };
//			} else {
//				// 实际有效代码位分布
//				zones = codeModule.array();
//			}
//			// 平衡分配
//			WCharBalancer balancer = new WCharBalancer();
//			for (int i = 0; i < zones.length; i++) {
//				balancer.add(zones[i]);
//			}
//			sector = balancer.balance(map.size());
//		} else {
//			IndexChart chart = indexModule.find(columnId);
//			// 1. 从集合中取出全部分片范围，同时过滤最大最小的分片
//			IndexZone[] zones = chart.find(true, java.lang.Short.MIN_VALUE, java.lang.Short.MAX_VALUE);
//			// 2. 分片保存到平衡器
//			ShortBalancer balancer = new ShortBalancer();
//			for (int i = 0; i < zones.length; i++) {
//				balancer.add(zones[i]);
//			}
//			// 3. 根据WORK节点数量，产生一个分片集合
//			sector = balancer.balance( map.size() );
//		}

		
		// 命名FROM输出，保存多个FromPhase
		FromOutputObject output = new FromOutputObject(SelectInitTask.SELECT_DIFFUSE);
		for(SiteHost host : froms.keySet()) {
			ChunkSet idset = froms.get(host);
			// 克隆SELECT，设置查找的chunkid
			Select clone_select = (Select)select.clone();
			long[] chunkids = idset.toArray();
			clone_select.setChunkids(chunkids);
			
			FromPhase phase = new FromPhase();
			phase.setRemote(host);
			phase.setSelect( clone_select );
			phase.setSlaveSector(sector);
			
			// 保存它
			output.addPhase(phase);
		}
		
		return output;
	}
	
	private ToOutputObject initTo(Select select) {
		Space space = select.getSpace();
		GroupBy gb = select.getGroupBy();
		OrderBy ob = select.getOrderBy();

		// 设置TO迭代
		ToOutputObject output = null;
		if (gb != null) {
			output = new ToOutputObject(SelectInitTask.SELECT_GROUPBY);
			
			// 1. 如果有ORDERBY，根据ORDERBY命名主机的数量确定最大分片数量
			ColumnSector sector = null;
			if (ob != null) {
				short columnId = ob.getColumnId();
				SiteSet slaveSites = getToChooser().findToSites(new Naming(SelectInitTask.SELECT_ORDERBY));
				int sites = slaveSites.size();
				sector = this.doSector(sites, space, columnId);
			}

			//2. 找GROUPBY主机
			SiteSet hosts = getToChooser().findToSites(new Naming(SelectInitTask.SELECT_GROUPBY));
			for (SiteHost host : hosts.list()) {
				ToPhase phase = new ToPhase();
				phase.setRemote(host);
				phase.addValue(new CObject("GROUPBY_STREAM", gb));
				if (sector != null) phase.setSlaveSector(sector);
				output.addPhase(phase);
			}
		}

		if (ob != null) {
			ToOutputObject object = new ToOutputObject(SelectInitTask.SELECT_ORDERBY);

			//1. 给ORDERBY找WORK主机
			SiteSet hosts = super.getToChooser().findToSites(new Naming(SelectInitTask.SELECT_ORDERBY));
			//2. 分配各单元 
			for(SiteHost host : hosts.list()) {
				ToPhase phase = new ToPhase();
				phase.setRemote(host);
				phase.addValue(new CObject("ORDERBY_STREAM", ob));
				object.addPhase(phase);
			}

			if (output == null) {
				output = object;
			} else {
				output.setLast(object);
			}
		}

		output.doLinkIndex();

		return output;
	}

	private BalanceObject initBalance(Select select) {
		return null;
	}
	
	private CollectObject initCollect(Select select) {
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.algorithm.init.InitTask#init(com.lexst.sql.statement.Conduct)
	 */
	@Override
	public Conduct init(Conduct conduct) throws InitTaskException {
		FromInputObject input = conduct.getFrom().getInput();
		if (input.countSelect() != 1) {
			Logger.error("SelectInitTask.init, select size != 1");
			throw new InitTaskException("sql select error!");
		}

		Select select = input.getSelect(0);
		// 根据SELECT，生成FROM对象
		FromOutputObject output = initFrom(select);
		conduct.getFrom().setOutput(output);
		// 根据SELECT，生成TO对象及链表
		ToOutputObject to = initTo(select);
		conduct.getTo().setOutput(to);
		// 平衡分配
		BalanceObject balance = initBalance(select);
		conduct.setBalance(balance);
		// 生成COLLECT显示
		CollectObject collect = initCollect(select);
		conduct.setCollect(collect);

		return conduct;
	}

//	/**
//	 * 初始化Conduct(分配资源等)
//	 * @param conduct
//	 * @return
//	 */
//	private Conduct init2( Select select) {
////		FromObject from = conduct.getFrom();
////		if (from.countSelect() != 1) {
////			Logger.error("SelectInitTask.init, select size > 1");
////			return null;
////		}
////
////		Select select = from.getSelect(0);
//		
//		Space space = select.getSpace();
//
//		// 找到Table, IndexModule, CodePointModule，写入配置
//		Table table = super.getTaskChooser().findTable(space);
//		if (table == null) {
//			Logger.error("SelectInitTask.init, cannot find %s", space);
//			return null;
//		}
//
//		// 查找索引分区
//		IndexModule module = super.getTaskChooser().findIndexModule(space);
//		if (module == null) {
//			Logger.error("SelectInitTask.init, cannot find module:%s", space);
//			return null;
//		}
//		
//		GroupBy group =	select.getGroupBy();
//		OrderBy order = select.getOrderBy();
//		
//		//1. 根据TO首对象中的分片定义，分配分片资源到FROM中
//		
//		//2. TO从对象开始，分配分片资源到它的上一级中
//
//
//		// 查找代码位分片
//		for (ColumnAttribute attribute : table.values()) {
//			if (attribute.isKey() && attribute.isWord()) {
//				Docket docket = new Docket(space, attribute.getColumnId());
//				CodeIndexModule cpm = super.getTaskChooser().findCodeIndexModule(docket);
////				CodePointPool.getInstance().findModule(docket);
////				if (cpm == null)
////					continue;
////				chooser.addCodePointModule(cpm);
//			}
//		}
//		// }
//
//		return null;
//	}

//	Conduct conduct = new Conduct();
//	// 以下三项需要在系统实现
//	conduct.setInit(new InitObject("SYSTEM_SELECT_INIT"));
//	conduct.setBalance(new BalanceObject("SYSTEM_SELECT_BALANCE"));
//	conduct.setCollect(new CollectObject("SYSTEM_SELECT_COLLECT"));
//	// 分配FROM命名任务(diffuse)
//	conduct.setFrom(new FromObject("SYSTEM_SELECT_FROM"));
//	conduct.getFrom().addSelect(select);
//	// 分配TO命名任务(aggregate)
//	ToObject root = null;
//	if (select.getGroupBy() != null) {
//		root = new ToObject("SYSTEM_SELECT_GROUPBY");
//	}
//	if (select.getOrderBy() != null) {
//		if (root == null) {
//			root = new ToObject("SYSTEM_SELECT_ORDERBY");
//		} else {
//			root.setLast(new ToObject("SYSTEM_SELECT_ORDERBY"));
//		}
//	}
//	conduct.setTo(root);

//	// 去接口中执行配置参数分配，如数据分片，再执行计算
//	return this.conduct(conduct);
	
	

//	InitChooser chooser = new InitChooser();
////	chooser.setTables( this.mapTable );
////	chooser.setIndexModule(  )
//	
//	List<Space> list = new ArrayList<Space>();
//	for (Select select : distribute.getFrom().getSelects()) {
//		Space space = select.getSpace();
//		list.add(space);
//	}
//	// 找到全部与SELECT对应的IndexModule, CodePointModule, 组成OBJECT数组，写入			
////	ArrayList<Object> array = new ArrayList<Object>();
//	for (Space space : list) {
//		Table table = findTable(space);
//		if (table == null) continue;
//		chooser.addTable(table);
//
//		IndexModule module = findModule(space);
//		if(module == null) continue;
//		
//		chooser.addIndexModule(module);
//		
////		if (module != null) {
////			array.add(module);
////		}
//		
//		for (ColumnAttribute attribute : table.values()) {
//			if (attribute.isKey() && attribute.isWord()) {
//				Deck deck = new Deck(space, attribute.getColumnId());
//				CodePointModule cpm = CodePointPool.getInstance().findModule(deck);
//				if(cpm == null) continue;
//				chooser.addCodePointModule(cpm);
////				if (cpm != null) array.add(cpm);
//			}
//		}
//	}
//
////	Object[] params = new Object[array.size()];
////	array.toArray(params);
	
	
	// 生成一个初始配置的选择器
//	InitChooser chooser = task.getChooser();
			
//	InitChooser chooser = this.getChooser(distribute);
//	if (distribute.getClass() == Conduct.class) {
//		distribute = task.init((Conduct) distribute, chooser);
//	} else if (distribute.getClass() == Direct.class) {
//		distribute = task.init((Direct) distribute, chooser);
//	} else {
//		return false;
//	}
	
//	this.initChooser(task, distribute);
}
