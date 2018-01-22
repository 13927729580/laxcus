/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com, All rights reserved
 * 
 * index module
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 1/2/2010
 * 
 * @see com.lexst.sql.chunk
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.chunk;

import java.io.*;
import java.util.*;

import com.lexst.log.client.*;
import com.lexst.sql.index.*;
import com.lexst.sql.index.chart.*;
import com.lexst.sql.index.range.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.util.host.*;

/**
 * HOME集群下，某个表的全部索引映像集合
 * 
 */
public class IndexModule implements Serializable {
	
	private static final long serialVersionUID = -6439624911505613690L;

	/** 表名 **/
	private Space space;

	/** 列ID -> 索引分布图谱 **/
	private Map<java.lang.Short, IndexChart> mapChart = new TreeMap<java.lang.Short, IndexChart>();

	/**
	 * default
	 */
	public IndexModule() {
		super();
	}

	/**
	 * @param space
	 */
	public IndexModule(Space space) {
		this();
		this.setSpace(space);
	}

	/**
	 * 设置表名
	 * @param s
	 */
	public void setSpace(Space s) {
		this.space = new Space(s);
	}
	
	/**
	 * 返回表名
	 * @return Space
	 */
	public Space getSpace() {
		return this.space;
	}

	/**
	 * 返回列ID集合
	 * @return Set<short>
	 */
	public Set<Short> keySet() {
		return mapChart.keySet();
	}

	/**
	 * 根据检索条件，统计匹配的chunkid，并写入内存
	 * 
	 * @param condi
	 * @param store
	 * @return
	 */
	public int find(Condition condi, ChunkSet store) {
		int count = 0;
		for(int i = 0; condi != null; i++) {
			ChunkSet set = new ChunkSet();
			int size = select(condi, set);

			switch (condi.getOutsideRelation()) {
			case Condition.AND:
				if (size < 0) return -1;
				store.AND(set);
				count += size;
				break;
			case Condition.OR:
				if (size > 0) {
					store.OR(set);
					count += size;
				}
				break;
			default:
				if (i == 0) {
					store.add(set);
					count = size;
				} else {
					throw new IllegalArgumentException("invalid condition relate");
				}
			}
			// 下一层检索条件
			condi = condi.getNext();
		}
		return count;
	}

	/**
	 * @param condi
	 * @param store
	 * @return int
	 */
	private int select(Condition condi, ChunkSet store) {
		int count = 0;
		while (condi != null) {
			WhereIndex whereIndex = condi.getValue();
			short columnId = whereIndex.getColumnId();
			IndexChart chart = mapChart.get(columnId);
			if (chart == null) {
				Logger.error("IndexModule.select, cannot find column identity:%d", columnId);
				return 0;
			}
			Set<Long> set = chart.find(condi);
			if (!set.isEmpty()) {
				if (condi.isAND()) {
					// 保留相同CHUNKID
					store.AND(set);
				} else if (condi.isOR()) {
					// 累加
					store.OR(set);
				} else {
					store.add(set);
				}
				count = set.size();
			}
			// 检查友集
			for (Condition partner : condi.getPartners()) {
				int size = this.select(partner, store);
				if (size < 1) return -1;
				count += size;
			}
			condi = condi.getNext();
		}
		return count;
	}

	/**
	 * save a index range from site
	 * @param host
	 * @param index
	 * 
	 * @return boolean
	 */
	public boolean add(SiteHost host, IndexRange index) {
		short columnId = index.getColumnId();
		IndexChart view = mapChart.get(columnId);
		if (index.isShort()) {
			if (view == null) {
				view = new ShortIndexChart();
				mapChart.put(columnId, view);
			}
		} else if (index.isInteger()) {
			if (view == null) {
				view = new IntegerIndexChart();
				mapChart.put(columnId, view);
			}
		} else if (index.isLong()) {
			if (view == null) {
				view = new LongIndexChart();
				mapChart.put(columnId, view);
			}
		} else if (index.isFloat()) {
			if (view == null) {
				view = new FloatIndexChart();
				mapChart.put(columnId, view);
			}
		} else if (index.isDouble()) {
			if (view == null) {
				view = new DoubleIndexChart();
				mapChart.put(columnId, view);
			}
		} else {
			throw new ClassCastException("illegal index class!");
		}
		// save index
		return view.add(host, index);
	}

	/**
	 * release a site record from chart set
	 * @param host
	 * @return int
	 */
	public int remove(SiteHost host) {
		int size = mapChart.size();
		if (size == 0) return 0;

		int count = 0;
		ArrayList<Short> a = new ArrayList<Short>(size);
		for (short columnId : mapChart.keySet()) {
			IndexChart chart = mapChart.get(columnId);
			if (chart != null) {
				count += chart.remove(host);
				if (chart.isEmpty()) a.add(columnId);
			} else {
				a.add(columnId);
			}
		}
		for (short columnId : a) {
			mapChart.remove(columnId);
		}
		return count;
	}

	/**
	 * 删除某台主机下的分布图谱，返回被删除的chunkid集合
	 * 
	 * @param host
	 * @return
	 */
	public List<Long> delete(SiteHost host) {
		int size = mapChart.size();
		if(size == 0) return null;

		ArrayList<Long> array = new ArrayList<Long>(1024);
		ArrayList<Short> a = new ArrayList<Short>(size);
		for(short columnId : mapChart.keySet()) {
			IndexChart view = mapChart.get(columnId);
			if(view == null) {
				a.add(columnId);
			} else {
				List<Long> list = view.delete(host);
				if (list != null) {
					array.addAll(list);
				}
				if(view.isEmpty()) a.add(columnId);
			}
		}
		for (short columnId : a) {
			mapChart.remove(columnId);
		}
		return array;
	}

	/**
	 * 根据列ID，查找对就的索引分布图谱
	 * 
	 * @param columnId
	 * @return
	 */
	public IndexChart find(short columnId) {
		return mapChart.get(columnId);
	}

	/**
	 * 索引图谱数量
	 * 
	 * @return int
	 */
	public int size() {
		return mapChart.size();
	}

	/**
	 * 是否空状态
	 * 
	 * @return boolean
	 */
	public boolean isEmpty() {
		return mapChart.isEmpty();
	}
}