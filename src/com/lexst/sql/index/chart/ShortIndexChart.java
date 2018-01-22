/**
 *
 */
package com.lexst.sql.index.chart;

import java.util.*;

import com.lexst.sql.index.*;
import com.lexst.sql.index.balance.*;
import com.lexst.sql.index.range.*;
import com.lexst.sql.statement.*;
import com.lexst.util.host.*;
import com.lexst.util.range.*;

/**
 * 基于某一列，短整型在HOME集群下的分布图
 *
 */
public class ShortIndexChart implements IndexChart {

	/** 索引范围 -> 主机地址集合 **/
	private Map<ShortRange, ChunkChart> charts = new TreeMap<ShortRange, ChunkChart>();

	/**
	 * default
	 */
	public ShortIndexChart() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#add(com.lexst.util.host.SiteHost, com.lexst.sql.index.range.IndexRange)
	 */
	@Override
	public boolean add(SiteHost host, IndexRange index) {
		if(index.getClass() != ShortIndexRange.class) {
			throw new ClassCastException("this is not ShortIndexRange class");
		}
		ShortIndexRange idx = (ShortIndexRange) index;
		short begin = idx.getBegin();
		short end = idx.getEnd();
		ShortRange range = new ShortRange(begin, end);
		long chunkId = idx.getChunkId();

		ChunkChart set = charts.get(range);
		if (set == null) {
			set = new ChunkChart();
			charts.put(range, set);
		}
		return set.add(host, chunkId);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#remove(com.lexst.util.host.SiteHost)
	 */
	@Override
	public int remove(SiteHost host) {
		int size = charts.size();
		if(size == 0) return size;

		int count = 0;
		ArrayList<ShortRange> a = new ArrayList<ShortRange>(size);
		for (ShortRange range : charts.keySet()) {
			ChunkChart set = charts.get(range);
			if (set != null) {
				count += set.remove(host);
				if (set.isEmpty()) a.add(range);
			} else {
				a.add(range);
			}
		}
		for (ShortRange range : a) {
			charts.remove(range);
		}
		return count;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#remove(com.lexst.util.host.SiteHost, long)
	 */
	@Override
	public int remove(SiteHost host, long chunkid) {
		int size = charts.size();
		if(size == 0) return 0;

		int count = 0;
		ArrayList<ShortRange> a = new ArrayList<ShortRange>(size);
		for (ShortRange range : charts.keySet()) {
			ChunkChart set = charts.get(range);
			if (set != null) {
				boolean success = set.remove(host, chunkid);
				if (success) count++;
				if (set.isEmpty()) a.add(range);
			} else {
				a.add(range);
			}
		}
		for (ShortRange range : a) {
			charts.remove(range);
		}
		return count;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#delete(com.lexst.util.host.SiteHost)
	 */
	@Override
	public List<Long> delete(SiteHost host) {
		int size = charts.size();
		if( size == 0) return null;

		ArrayList<Long> array = new ArrayList<Long>(256);

		ArrayList<ShortRange> a = new ArrayList<ShortRange>(size);
		for (ShortRange range : charts.keySet()) {
			ChunkChart set = charts.get(range);
			if (set != null) {
				set.remove(host, array);
				if (set.isEmpty()) a.add(range);
			} else {
				a.add(range);
			}
		}
		for (ShortRange range : a) {
			charts.remove(range);
		}
		return array;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#find(com.lexst.sql.statement.Condition)
	 */
	@Override
	public Set<Long> find(Condition condi) {
		WhereIndex whereIndex = condi.getValue();
		if(whereIndex == null || whereIndex.getClass() != ShortIndex.class) {
			throw new IllegalArgumentException("null pointer or invalid index!");
		}
		short value = ((ShortIndex) whereIndex).getValue();

		// 找到范围分片截止点
		HashSet<Long> array = new HashSet<Long>(1024);
		// 列名在左,参数值在右(固定!, 检查可以的范围)
		switch (condi.getCompare()) {
		case Condition.EQUAL:
			for (ShortRange range : charts.keySet()) {
				if (range.inside(value)) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.NOT_EQUAL:
			for(ShortRange range : charts.keySet()) {
				if (range.getBegin() != value || range.getEnd() != value) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.LESS:
			for(ShortRange range : charts.keySet()) {
				if (range.getBegin() < value || range.getEnd() < value) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.LESS_EQUAL:
			for(ShortRange range : charts.keySet()) {
				if (range.getBegin() <= value || range.getEnd() <= value) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.GREATER:
			for(ShortRange range : charts.keySet()) {
				if (range.getBegin() > value || range.getEnd() > value) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.GREATER_EQUAL:
			for(ShortRange range : charts.keySet()) {
				if (range.getBegin() >= value || range.getEnd() >= value) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.LIKE:
			for (ChunkChart set : charts.values()) {
				array.addAll(set.keySet());
			}
			break;
		}
		return array;
	}
	
//	/**
//	 * inspect
//	 * 查找全部索引集合，返回它的范围和权重
//	 * @param ignore - 如果TRUE，以下两个参数有效，否则不处理
//	 * @param ignoreBegin
//	 * @param ignoreEnd
//	 * @return
//	 */
//	@Override
//	public IndexZone[] find(boolean ignore, java.lang.Number ignoreBegin, java.lang.Number ignoreEnd) {
//		List<ShortZone> array = new ArrayList<ShortZone>(charts.size());
//		
//		Iterator<Map.Entry<ShortRange, ChunkChart>> iterators = charts.entrySet().iterator();
//		if (ignore) {
//			if (ignoreBegin.getClass() != java.lang.Short.class || ignoreEnd.getClass() != java.lang.Short.class) {
//				throw new ClassCastException("this not java.lang.Short class!");
//			}
//			ShortRange range = new ShortRange(
//					((java.lang.Short) ignoreBegin).shortValue(),
//					((java.lang.Short) ignoreEnd).shortValue());
//			while (iterators.hasNext()) {
//				Map.Entry<ShortRange, ChunkChart> entry = iterators.next();
//				if (range.equals(entry.getKey())) continue;
//				ShortZone si = new ShortZone(entry.getKey(), entry.getValue().size());
//				array.add((ShortZone) si.clone());
//			}
//		} else {
//			while (iterators.hasNext()) {
//				Map.Entry<ShortRange, ChunkChart> entry = iterators.next();
//				ShortZone si = new ShortZone(entry.getKey(), entry.getValue().size());
//				array.add((ShortZone) si.clone());
//			}
//		}
//		int size = array.size();
//		ShortZone[] s = new ShortZone[size];
//		return array.toArray(s);
//	}

	/**
	 * 筛选索引区(过滤不符合条件的，保留符合条件的)
	 * 
	 * @param ignore - 是否过滤
	 * @param b - 开始位置下标
	 * @param e - 结束位置下标
	 * @return
	 */
	@Override
	public IndexZone[] choice(boolean ignore, Number[] b, Number[] e) {
		ShortRange[] ranges = null;
		if (ignore) {
			if (b == null || e == null) {
				throw new NullPointerException("number array null pointer");
			} else if (b.length != e.length) {
				throw new IllegalArgumentException("number size not match!");
			}
			ranges = new ShortRange[b.length];
			for (int i = 0; i < b.length; i++) {
				if (b[i].getClass() != java.lang.Short.class || e[i].getClass() != java.lang.Short.class) {
					throw new ClassCastException("this not java.lang.Short class!");
				}
				ranges[i] = new ShortRange(((java.lang.Short) b[i]).shortValue(), ((java.lang.Short) e[i]).shortValue());
			}
		}

		List<ShortZone> array = new ArrayList<ShortZone>(charts.size());
		Iterator<Map.Entry<ShortRange, ChunkChart>> iterators = charts.entrySet().iterator();
		while (iterators.hasNext()) {
			Map.Entry<ShortRange, ChunkChart> entry = iterators.next();
			boolean skip = false;
			for (int i = 0; ranges != null && i < ranges.length; i++) {
				skip = (ranges[i].equals(entry.getKey()));
				if (skip) break;
			}
			if (skip) continue;
			ShortZone si = new ShortZone(entry.getKey(), entry.getValue().size());
			array.add((ShortZone) si.clone());
		}

		ShortZone[] s = new ShortZone[array.size()];
		return array.toArray(s);
	}
	
	/**
	 * 筛选索引区
	 * 
	 * @param ignore - is true, 过滤标准索引区(是小和最大值)
	 * @return
	 */
	@Override
	public IndexZone[] choice(boolean ignore) {
		if (ignore) {
			return choice(true, new java.lang.Short[] { java.lang.Short.MIN_VALUE },
					new java.lang.Short[] { java.lang.Short.MAX_VALUE });
		} else {
			return this.choice(false, null, null);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return charts.isEmpty();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#size()
	 */
	@Override
	public int size() {
		return charts.size();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#getChunkIds()
	 */
	@Override
	public Set<Long> getChunkIds() {
		Set<Long> array = new TreeSet<Long>();
		for (ChunkChart set : charts.values()) {
			array.addAll(set.keySet());
		}
		return array;
	}

}