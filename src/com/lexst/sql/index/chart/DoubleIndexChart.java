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
 * 双浮点索引区在HOME集群下的分布图谱
 * 
 */
public class DoubleIndexChart implements IndexChart {

	/** 双浮点索引范围  -> CHUNK主机地址集合 **/
	private Map<DoubleRange, ChunkChart> charts = new TreeMap<DoubleRange, ChunkChart>();

	/**
	 * default
	 */
	public DoubleIndexChart() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.lexst.call.index.set.IndexSet#add(com.lexst.call.index.Index)
	 */
	@Override
	public boolean add(SiteHost host, IndexRange index) {
		if (index.getClass() != DoubleIndexRange.class) {
			throw new java.lang.ClassCastException("not double index");
		}
		DoubleIndexRange idx = (DoubleIndexRange) index;
		double begin = idx.getBegin();
		double end = idx.getEnd();
		DoubleRange range = new DoubleRange(begin, end);
		long chunkId = idx.getChunkId();

		ChunkChart set = charts.get(range);
		if (set == null) {
			set = new ChunkChart();
			charts.put(range, set);
		}
		return set.add(host, chunkId);
	}

	/**
	 *
	 */
	@Override
	public int remove(SiteHost host) {
		int size = charts.size();
		if(size == 0) return size;
		int count = 0;
		ArrayList<DoubleRange> a = new ArrayList<DoubleRange>(size);
		for (DoubleRange range : charts.keySet()) {
			ChunkChart set = charts.get(range);
			if (set != null) {
				count += set.remove(host);
				if (set.isEmpty()) a.add(range);
			} else {
				a.add(range);
			}
		}
		for (DoubleRange range : a) {
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

		ArrayList<DoubleRange> a = new ArrayList<DoubleRange>(size);
		for (DoubleRange range : charts.keySet()) {
			ChunkChart set = charts.get(range);
			if (set != null) {
				set.remove(host, array);
				if (set.isEmpty()) a.add(range);
			} else {
				a.add(range);
			}
		}
		for (DoubleRange range : a) {
			charts.remove(range);
		}
		return array;
	}

	/* (non-Javadoc)
	 * @see com.lexst.call.index.set.IndexSet#remove(long)
	 */
	@Override
	public int remove(SiteHost host, long chunkId) {
		int size = charts.size();
		if(size == 0) return 0;

		int count = 0;
		ArrayList<DoubleRange> a = new ArrayList<DoubleRange>(size);
		for (DoubleRange range : charts.keySet()) {
			ChunkChart set = charts.get(range);
			if (set != null) {
				boolean success = set.remove(host, chunkId);
				if (success) count++;
				if (set.isEmpty()) a.add(range);
			} else {
				a.add(range);
			}
		}
		for (DoubleRange range : a) {
			charts.remove(range);
		}
		return count;
	}

	/* (non-Javadoc)
	 * @see com.lexst.call.index.set.IndexSet#find(com.lexst.call.index.Condition)
	 */
	@Override
	public Set<Long> find(Condition condi) {
		WhereIndex whereIndex = condi.getValue();
		if (whereIndex == null || whereIndex.getClass() != DoubleIndex.class) {
			throw new IllegalArgumentException("null pointer or invalid index");
		}
		double value = ((DoubleIndex) whereIndex).getValue();

		// 找到范围分片截止点
		HashSet<Long> all = new HashSet<Long>(1024);
		// 列名在左,参数值在右(固定!, 检查可以的范围)
		switch (condi.getCompare()) {
		case Condition.EQUAL:
			for (DoubleRange range : charts.keySet()) {
				if (range.inside(value)) {
					ChunkChart set = charts.get(range);
					all.addAll(set.keySet());
				}
			}
			break;
		case Condition.NOT_EQUAL:
			for(DoubleRange range : charts.keySet()) {
				if (range.getBegin() != value || range.getEnd() != value) {
					ChunkChart set = charts.get(range);
					all.addAll(set.keySet());
				}
			}
			break;
		case Condition.LESS:
			for(DoubleRange range : charts.keySet()) {
				if (range.getBegin() < value || range.getEnd() < value) {
					ChunkChart set = charts.get(range);
					all.addAll(set.keySet());
				}
			}
			break;
		case Condition.LESS_EQUAL:
			for(DoubleRange range : charts.keySet()) {
				if (range.getBegin() <= value || range.getEnd() <= value) {
					ChunkChart set = charts.get(range);
					all.addAll(set.keySet());
				}
			}
			break;
		case Condition.GREATER:
			for(DoubleRange range : charts.keySet()) {
				if (range.getBegin() > value || range.getEnd() > value) {
					ChunkChart set = charts.get(range);
					all.addAll(set.keySet());
				}
			}
			break;
		case Condition.GREATER_EQUAL:
			for(DoubleRange range : charts.keySet()) {
				if (range.getBegin() >= value || range.getEnd() >= value) {
					ChunkChart set = charts.get(range);
					all.addAll(set.keySet());
				}
			}
			break;
		case Condition.LIKE:
			for (ChunkChart set : charts.values()) {
				all.addAll(set.keySet());
			}
			break;
		}
		return all;
	}

//	/*
//	 * (non-Javadoc)
//	 * @see com.lexst.sql.index.chart.IndexChart#find(boolean, java.lang.Number, java.lang.Number)
//	 */
//	@Override
//	public IndexZone[] find(boolean ignore, java.lang.Number ignoreBegin, java.lang.Number ignoreEnd) {
//		List<DoubleZone> array = new ArrayList<DoubleZone>(charts.size());
//		
//		Iterator<Map.Entry<DoubleRange, ChunkChart>> iterators = charts.entrySet().iterator();
//		if (ignore) {
//			if (ignoreBegin.getClass() != java.lang.Double.class || ignoreEnd.getClass() != java.lang.Double.class) {
//				throw new ClassCastException("this not java.lang.Double class!");
//			}
//			DoubleRange range = new DoubleRange(
//					((java.lang.Double) ignoreBegin).intValue(),
//					((java.lang.Double) ignoreEnd).intValue());
//			while (iterators.hasNext()) {
//				Map.Entry<DoubleRange, ChunkChart> entry = iterators.next();
//				if (range.equals(entry.getKey())) continue;
//				DoubleZone zone = new DoubleZone(entry.getKey(), entry.getValue().size());
//				array.add(zone);
//			}
//		} else {
//			while (iterators.hasNext()) {
//				Map.Entry<DoubleRange, ChunkChart> entry = iterators.next();
//				DoubleZone zone = new DoubleZone(entry.getKey(), entry.getValue().size());
//				array.add(zone);
//			}
//		}
//		int size = array.size();
//		DoubleZone[] s = new DoubleZone[size];
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
		DoubleRange[] ranges = null;
		if (ignore) {
			if (b == null || e == null) {
				throw new NullPointerException("number array null pointer");
			} else if (b.length != e.length) {
				throw new IllegalArgumentException("number size not match!");
			}
			ranges = new DoubleRange[b.length];
			for (int i = 0; i < b.length; i++) {
				if (b[i].getClass() != java.lang.Double.class || e[i].getClass() != java.lang.Double.class) {
					throw new ClassCastException("this not java.lang.Double class!");
				}
				ranges[i] = new DoubleRange(((java.lang.Double) b[i]).doubleValue(), ((java.lang.Double) e[i]).doubleValue() );
			}
		}

		List<DoubleZone> array = new ArrayList<DoubleZone>(charts.size());
		Iterator<Map.Entry<DoubleRange, ChunkChart>> iterators = charts.entrySet().iterator();
		while (iterators.hasNext()) {
			Map.Entry<DoubleRange, ChunkChart> entry = iterators.next();
			boolean skip = false;
			for (int i = 0; ranges != null && i < ranges.length; i++) {
				skip = (ranges[i].equals(entry.getKey()));
				if (skip) break;
			}
			if (skip) continue;
			DoubleZone si = new DoubleZone(entry.getKey(), entry.getValue().size());
			array.add((DoubleZone) si.clone());
		}

		DoubleZone[] s = new DoubleZone[array.size()];
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
			return choice(true, new java.lang.Double[] { java.lang.Double.MIN_VALUE },
					new java.lang.Double[] { java.lang.Double.MAX_VALUE });
		} else {
			return this.choice(false, null, null);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.call.index.set.IndexSet#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return charts.isEmpty();
	}

	/* (non-Javadoc)
	 * @see com.lexst.call.index.set.IndexSet#size()
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