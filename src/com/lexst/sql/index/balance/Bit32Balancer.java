/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.balance;

import java.util.*;

import com.lexst.sql.index.range.*;
import com.lexst.sql.index.section.*;
import com.lexst.util.range.*;

/**
 * 整型分布区平衡分割器。<br>
 * 整型索引分布范围（可以衔接，但是不能重叠，发生重叠情况将合并)<br>
 * 
 */
abstract class Bit32Balancer implements IndexBalancer {
	
	private static final long serialVersionUID = 6550828741219713848L;

	/** 一组可以衔接，但是不能重叠的整型值集合  **/
	protected ArrayList<IntegerZone> array = new ArrayList<IntegerZone>();
	
	/**
	 * default
	 */
	protected Bit32Balancer() {
		super();
	}
	
	public List<IntegerZone> list() {
		return array;
	}
	
	public int size() {
		return this.array.size();
	}
	
	public boolean isEmpty() {
		return this.array.isEmpty();
	}

	
//	private IntegerRange split(int index, int unit) {
//		
//		return null;
//	}
//	
//	/**
//	 * 相连的分片进行合并，到指定的数量
//	 */
//	public void accord(int sites) {
//		java.util.Collections.sort(array);
//
//		IntegerRange[] ranges = new IntegerRange[sites];
//		// 单元数
//		int unit = array.size() / sites;
//		if (array.size() % sites != 0) unit += 1;
//
//		for (int index = 0, j = 0; index < array.size(); index += unit) {
//			// 取指定下标开始，取N个分组，进行合并
//			ranges[j] = split(index, unit);
//		}
//
//		array.clear();
//		for (int i = 0; i < ranges.length; i++) {
//			array.add(ranges[i]);
//		}
//	}
	
	/**
	 * 扩大分布范围数量：逐次找到统计频率最大分布，分成两部分，再保存!
	 * @param sites
	 */
	private void extend(int sites) {
		while(array.size() < sites) {
			java.util.Collections.sort(array);
			// 找到频率值最大的分片，分割成两部分
			int index = -1, count = -1;
			for(int i = 0; i < array.size(); i++) {
				IntegerZone view = array.get(i);
				if(view.getWeight() > count) {
					index = i;
					count = view.getWeight();
				}
			}
			IntegerZone zone = array.get(index); 
			IntegerRange[] ranges = zone.getRange().split(2);
			int middle = zone.getWeight() / 2;
			
			// 删除旧的
			array.remove(index);
			// 增加分隔的新值
			this.add(new Integer(ranges[0].getBegin()), new Integer(ranges[0].getEnd()), middle);
			this.add(new Integer(ranges[1].getBegin()), new Integer(ranges[1].getEnd()), zone.getWeight() - middle);
		}
		java.util.Collections.sort(array);
	}
	
	/**
	 * 缩小分布范围数目：找到衔接的两个分片，合为一体
	 * @param sites
	 */
	private void shrink(int sites) {
		while (array.size() > sites) {
			// 必须大于1才可以比较
			if (array.size() < 2) break;
			java.util.Collections.sort(array);
			// 找到统计值最小的
			int index = 0, count = 0;
			for (int i = 0; i < array.size(); i++) {
				IntegerZone volumn = array.get(i);
				if (i == 0 || volumn.getWeight() < count) {
					index = i;
					count = volumn.getWeight();
				}
			}

			if (index == 0) {
				// 与它之后的合并
				IntegerZone v1 = array.get(0);
				IntegerZone v2 = array.get(1);

				IntegerZone zone = new IntegerZone(v1.getRange().getBegin(), v2.getRange().getEnd(), v1.getWeight());
				zone.addWeight(v2.getWeight());
				
				array.remove(v1);
				array.remove(v2);
				array.add(zone);
			} else if (index + 1 == array.size()) {
				// 与它前面的合并
				IntegerZone v1 = array.get(index - 1);
				IntegerZone v2 = array.get(index);

				IntegerZone zone = new IntegerZone(v1.getRange().getBegin(), v2.getRange().getEnd(), v1.getWeight());
				zone.addWeight(v2.getWeight());
				
				array.remove(v1);
				array.remove(v2);
				array.add(zone);
			} else {
				// 与它后面的合并
				IntegerZone v1 = array.get(index);
				IntegerZone v2 = array.get(index + 1);

				IntegerZone zone = new IntegerZone(v1.getRange().getBegin(), v2.getRange().getEnd(), v1.getWeight());
				zone.addWeight(v2.getWeight());
				
				array.remove(v1);
				array.remove(v2);
				array.add(zone);
			}
		}
		java.util.Collections.sort(array);
	}

	/**
	 * 合并衔接的分布
	 */
	private void join() {
		java.util.Collections.sort(array);
		for (int index = 0; index < array.size() - 1; index++) {
			if (index + 2 > array.size()) break;
			
			IntegerZone v1 = array.get(index);
			IntegerZone v2 = array.get(index + 1);
			IntegerRange r1 = v1.getRange();
			IntegerRange r2 = v2.getRange();
			if (r1.getEnd() < r2.getBegin()) {
				continue; // 不关联，继续下一个比较
			}
			
			// 开始位置取最小值，结束位置取最大值
			int begin = (r1.getBegin() < r2.getBegin() ? r1.getBegin() : r2.getEnd());
			int end = (r1.getEnd() > r2.getEnd() ? r1.getEnd() : r2.getEnd());

			IntegerZone zone = new IntegerZone(begin, end, v1.getWeight());
			zone.addWeight(v2.getWeight());
			
			array.remove(v1);
			array.remove(v2);
			array.add(zone);
			index = -1; // 从新开始比较
			java.util.Collections.sort(array);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.balance.IndexBalancer#balance(int)
	 */
	@Override
	public ColumnSector balance(int sites) {
		// 将存在衔接的情况进行合并
		this.join();
		// 如果当前片段数量小于主机数，扩展片段到主机数
		if(array.size() < sites) {
			this.extend(sites);
		} else if(array.size() > sites) {
			// 如果当前片段数量大于主机数，缩小片段到主机数（允许不到主机数目）
			this.shrink(sites);
		}

		// 输出分片范围
		Bit32Sector sector = getSector();
		for(IntegerZone volumn : array) {
			sector.add(volumn.getRange());
		}
		return sector;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.IndexBalancer#add(com.lexst.sql.index.balance.BalanceView)
	 */
	@Override
	public boolean add(IndexZone view) {
		if (view.getClass() != IntegerZone.class) {
			throw new java.lang.ClassCastException("this is not IntegerZone class");
		}
		return array.add((IntegerZone) view);
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.IndexBalancer#add(com.lexst.sql.index.range.IndexRange, int)
	 */
	@Override
	public boolean add(IndexRange range, int count) {
		if (range.getClass() != IntegerIndexRange.class) {
			throw new ClassCastException("class case error! cannot IntegerIndexRange");
		}
		IntegerIndexRange idx = (IntegerIndexRange) range;
		return add(new IntegerZone(idx.getBegin(), idx.getEnd(), count));
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.IndexBalancer#add(java.lang.Number, java.lang.Number, int)
	 */
	@Override
	public boolean add(Number begin, Number end, int count) {
		if (begin.getClass() != Integer.class || end.getClass() != Integer.class) {
			throw new java.lang.ClassCastException("this is not java.lang.Integer class");
		}		
		return add(new IntegerZone(((Integer) begin).intValue(), ((Integer) end).intValue(), count));
	}

	/**
	 * 返回一个Bit32Sector子类实例. 子类实现
	 * @return
	 */
	public abstract Bit32Sector getSector();
}