/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.index.balance;

import java.util.*;

import com.lexst.sql.index.range.*;
import com.lexst.sql.index.section.*;
import com.lexst.util.range.*;

/**
 * 短整型平衡分割器。<br>
 * 整型索引分布范围（可以衔接，但是不能重叠，发生重叠情况将合并) <br>
 * 
 */
public class ShortBalancer implements IndexBalancer {
	
	private static final long serialVersionUID = -8905028127460184460L;
	
	/** 一组可以衔接，但是不能重叠的整型值集合  **/
	private ArrayList<ShortZone> array = new ArrayList<ShortZone>();
	
	/**
	 * default
	 */
	public ShortBalancer() {
		super();
	}
	
	public List<ShortZone> list() {
		return array;
	}
	
	public int size() {
		return this.array.size();
	}
	
	public boolean isEmpty() {
		return this.array.isEmpty();
	}
	
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
				ShortZone view = array.get(i);
				if(view.getWeight() > count) {
					index = i;
					count = view.getWeight();
				}
			}
			ShortZone zone = array.get(index); 
			ShortRange[] ranges = zone.getRange().split(2);
			int middle = zone.getWeight() / 2;
			
			// 删除旧的
			array.remove(index);
			// 增加分隔的新值
			this.add(new Short(ranges[0].getBegin()), new Short(ranges[0].getEnd()), middle);
			this.add(new Short(ranges[1].getBegin()), new Short(ranges[1].getEnd()), zone.getWeight() - middle);
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
				ShortZone volumn = array.get(i);
				if (i == 0 || volumn.getWeight() < count) {
					index = i;
					count = volumn.getWeight();
				}
			}

			if (index == 0) {
				// 与它之后的合并
				ShortZone v1 = array.get(0);
				ShortZone v2 = array.get(1);

				ShortZone zone = new ShortZone(v1.getRange().getBegin(), v2.getRange().getEnd(), v1.getWeight());
				zone.addWeight(v2.getWeight());
				
				array.remove(v1);
				array.remove(v2);
				array.add(zone);
			} else if (index + 1 == array.size()) {
				// 与它前面的合并
				ShortZone v1 = array.get(index - 1);
				ShortZone v2 = array.get(index);

				ShortZone zone = new ShortZone(v1.getRange().getBegin(), v2.getRange().getEnd(), v1.getWeight());
				zone.addWeight(v2.getWeight());
				
				array.remove(v1);
				array.remove(v2);
				array.add(zone);
			} else {
				// 与它后面的合并
				ShortZone v1 = array.get(index);
				ShortZone v2 = array.get(index + 1);

				ShortZone zone = new ShortZone(v1.getRange().getBegin(), v2.getRange().getEnd(), v1.getWeight());
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
			
			ShortZone v1 = array.get(index);
			ShortZone v2 = array.get(index + 1);
			ShortRange r1 = v1.getRange();
			ShortRange r2 = v2.getRange();
			if (r1.getEnd() < r2.getBegin()) {
				continue; // 不关联，继续下一个比较
			}
			
			// 开始位置取最小值，结束位置取最大值
			short begin = (r1.getBegin() < r2.getBegin() ? r1.getBegin() : r2.getEnd());
			short end = (r1.getEnd() > r2.getEnd() ? r1.getEnd() : r2.getEnd());

			ShortZone zone = new ShortZone(begin, end, v1.getWeight());
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
		ShortSector sector = new ShortSector();
		for (ShortZone zone : array) {
			sector.add(zone.getRange());
		}
		return sector;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.IndexBalancer#add(com.lexst.sql.index.balance.BalanceView)
	 */
	@Override
	public boolean add(IndexZone zone) {
		if (zone.getClass() != ShortZone.class) {
			throw new java.lang.ClassCastException("this is not ShortZone class");
		}
		return array.add((ShortZone) zone);
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.IndexBalancer#add(com.lexst.sql.index.range.IndexRange, int)
	 */
	@Override
	public boolean add(IndexRange range, int count) {
		if (range.getClass() != ShortIndexRange.class) {
			throw new java.lang.ClassCastException("this is not ShortIndexRange class");
		}
		ShortIndexRange idx = (ShortIndexRange) range;
		return add(new ShortZone(idx.getBegin(), idx.getEnd(), count));
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.IndexBalancer#add(java.lang.Number, java.lang.Number, int)
	 */
	@Override
	public boolean add(Number begin, Number end, int weight) {
		if (begin.getClass() != java.lang.Short.class || end.getClass() != java.lang.Short.class) {
			throw new java.lang.ClassCastException("this is not java.lang.Short class");
		}		
		return add(new ShortZone(((Short) begin).shortValue(), ((Short) end).shortValue(), weight));
	}
}