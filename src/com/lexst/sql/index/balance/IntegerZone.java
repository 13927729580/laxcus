/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.index.balance;

import com.lexst.util.range.*;

/**
 * 整型值索引分布区域
 * 
 */
public class IntegerZone extends IndexZone implements Comparable<IntegerZone> {

	private static final long serialVersionUID = 717957290801831114L;
	
	/** 数据值分布范围 **/
	private IntegerRange range;

	/**
	 * 
	 * @param range
	 * @param weight
	 */
	public IntegerZone(IntegerRange range, int weight) {
		super();
		this.setRange(range);
		super.setWeight(weight);
	}

	/**
	 * @param begin
	 * @param end
	 * @param weight
	 */
	public IntegerZone(int begin, int end, int weight) {
		this(new IntegerRange(begin, end), weight);
	}

	/**
	 * @param zone
	 */
	public IntegerZone(IntegerZone zone) {
		super(zone);
		this.setRange(zone.range);
	}

	/**
	 * 数值分布范围
	 * 
	 * @return
	 */
	public IntegerRange getRange() {
		return this.range;
	}

	/**
	 * @param range
	 */
	public void setRange(IntegerRange range) {
		this.range = new IntegerRange(range);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%d,%d %d", range.getBegin(), range.getEnd(), super.getWeight());
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != IntegerZone.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((IntegerZone) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.range.hashCode() ^ super.getWeight();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(IntegerZone zone) {
		return this.range.compareTo(zone.range);
	}

	/*
	 * 克隆对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new IntegerZone(this);
	}
}