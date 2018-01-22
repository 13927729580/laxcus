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

public class LongIndexChart implements IndexChart {

	/** LONG索引范围 -> 数据块分布地址 */
	private Map<LongRange, ChunkChart> charts = new TreeMap<LongRange, ChunkChart>();

	/**
	 * default constructor
	 */
	public LongIndexChart() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#add(com.lexst.util.host.SiteHost, com.lexst.sql.index.range.IndexRange)
	 */
	@Override
	public boolean add(SiteHost host, IndexRange index) {
		if(index.getClass() != LongIndexRange.class) {
			throw new ClassCastException("not big index");
		}
		LongIndexRange idx = (LongIndexRange)index;
		long begin = idx.getBegin();
		long end = idx.getEnd();
		LongRange range = new LongRange(begin, end);
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
		if(size == 0) return 0;

		int count = 0;
		ArrayList<LongRange> a = new ArrayList<LongRange>(size);
		for (LongRange range : charts.keySet()) {
			ChunkChart set = charts.get(range);
			if (set != null) {
				count += set.remove(host);
				if (set.isEmpty()) a.add(range);
			} else {
				a.add(range);
			}
		}
		for (LongRange range : a) {
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

		ArrayList<Long> array = new ArrayList<Long>(512);

		ArrayList<LongRange> a = new ArrayList<LongRange>(size);
		for (LongRange range : charts.keySet()) {
			ChunkChart set = charts.get(range);
			if (set != null) {
				set.remove(host, array);
				if (set.isEmpty()) a.add(range);
			} else {
				a.add(range);
			}
		}
		for (LongRange range : a) {
			charts.remove(range);
		}
		return array;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#remove(com.lexst.util.host.SiteHost, long)
	 */
	@Override
	public int remove(SiteHost host, long chunkId) {
		int size = charts.size();
		if(size == 0) return 0;

		int count = 0;
		ArrayList<LongRange> a = new ArrayList<LongRange>(size);
		for (LongRange range : charts.keySet()) {
			ChunkChart set = charts.get(range);
			if (set != null) {
				boolean success = set.remove(host, chunkId);
				if (success) count++;
				if (set.isEmpty()) a.add(range);
			} else {
				a.add(range);
			}
		}
		for (LongRange range : a) {
			charts.remove(range);
		}
		return count;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.chart.IndexChart#find(com.lexst.sql.statement.Condition)
	 */
	@Override
	public Set<Long> find(Condition condi) {
		WhereIndex whereIndex = condi.getValue();
		if (whereIndex == null || whereIndex.getClass() != LongIndex.class) {
			throw new IllegalArgumentException("null pointer or invalid type!");
		}
		long value = ((LongIndex) whereIndex).getValue();

		// 找到范围分片截止点
		HashSet<Long> array = new HashSet<Long>(1024);
		// 列名在左,参数值在右(固定!, 检查可以的范围)
		switch (condi.getCompare()) {
		case Condition.EQUAL:
			for (LongRange range : charts.keySet()) {
				if (range.inside(value)) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.NOT_EQUAL:
			for(LongRange range : charts.keySet()) {
				if (range.getBegin() != value || range.getEnd() != value) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.LESS:
			for(LongRange range : charts.keySet()) {
				if (range.getBegin() < value || range.getEnd() < value) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.LESS_EQUAL:
			for(LongRange range : charts.keySet()) {
				if (range.getBegin() <= value || range.getEnd() <= value) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.GREATER:
			for(LongRange range : charts.keySet()) {
				if (range.getBegin() > value || range.getEnd() > value) {
					ChunkChart set = charts.get(range);
					array.addAll(set.keySet());
				}
			}
			break;
		case Condition.GREATER_EQUAL:
			for(LongRange range : charts.keySet()) {
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

//	/*
//	 * (non-Javadoc)
//	 * @see com.lexst.sql.index.chart.IndexChart#find(boolean, java.lang.Number, java.lang.Number)
//	 */
//	@Override
//	public IndexZone[] find(boolean ignore, java.lang.Number ignoreBegin, java.lang.Number ignoreEnd) {
//		List<LongZone> array = new ArrayList<LongZone>(charts.size());
//		
//		Iterator<Map.Entry<LongRange, ChunkChart>> iterators = charts.entrySet().iterator();
//		if (ignore) {
//			if (ignoreBegin.getClass() != java.lang.Long.class || ignoreEnd.getClass() != java.lang.Long.class) {
//				throw new ClassCastException("this not java.lang.Long class!");
//			}
//			LongRange range = new LongRange(
//					((java.lang.Long) ignoreBegin).intValue(),
//					((java.lang.Long) ignoreEnd).intValue());
//			while (iterators.hasNext()) {
//				Map.Entry<LongRange, ChunkChart> entry = iterators.next();
//				if (range.equals(entry.getKey())) continue;
//				LongZone zone = new LongZone(entry.getKey(), entry.getValue().size());
//				array.add(zone);
//			}
//		} else {
//			while (iterators.hasNext()) {
//				Map.Entry<LongRange, ChunkChart> entry = iterators.next();
//				LongZone zone = new LongZone(entry.getKey(), entry.getValue().size());
//				array.add(zone);
//			}
//		}
//		int size = array.size();
//		LongZone[] s = new LongZone[size];
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
		LongRange[] ranges = null;
		if (ignore) {
			if (b == null || e == null) {
				throw new NullPointerException("number array null pointer");
			} else if (b.length != e.length) {
				throw new IllegalArgumentException("number size not match!");
			}
			ranges = new LongRange[b.length];
			for (int i = 0; i < b.length; i++) {
				if (b[i].getClass() != java.lang.Long.class || e[i].getClass() != java.lang.Long.class) {
					throw new ClassCastException("this not java.lang.Long class!");
				}
				ranges[i] = new LongRange(((java.lang.Long) b[i]).longValue(), ((java.lang.Long) e[i]).longValue() );
			}
		}

		List<LongZone> array = new ArrayList<LongZone>(charts.size());
		Iterator<Map.Entry<LongRange, ChunkChart>> iterators = charts.entrySet().iterator();
		while (iterators.hasNext()) {
			Map.Entry<LongRange, ChunkChart> entry = iterators.next();
			boolean skip = false;
			for (int i = 0; ranges != null && i < ranges.length; i++) {
				skip = (ranges[i].equals(entry.getKey()));
				if (skip) break;
			}
			if (skip) continue;
			LongZone si = new LongZone(entry.getKey(), entry.getValue().size());
			array.add((LongZone) si.clone());
		}

		LongZone[] s = new LongZone[array.size()];
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
			return choice(true, new java.lang.Long[] { java.lang.Long.MIN_VALUE },
					new java.lang.Long[] { java.lang.Long.MAX_VALUE });
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
		Set<Long> all = new TreeSet<Long>();
		for (ChunkChart set : charts.values()) {
			all.addAll(set.keySet());
		}
		return all;
	}

}