/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.index.balance;

import com.lexst.util.range.*;

/**
 * @author scott.liang
 * 
 */
public class LongZone extends IndexZone implements Comparable<LongZone> {

	private static final long serialVersionUID = 332479566394862602L;
	
	/** 数据值分布范围 **/
	private LongRange range;

	/**
	 * 
	 * @param range
	 * @param count
	 */
	public LongZone(LongRange range, int count) {
		super();
		this.setRange(range);
		super.setWeight(count);
	}

	/**
	 * @param begin
	 * @param end
	 * @param count
	 */
	public LongZone(long begin, long end, int count) {
		this(new LongRange(begin, end), count);
	}

	/**
	 * @param zone
	 */
	public LongZone(LongZone zone) {
		super(zone);
		this.setRange(zone.range);
	}

	/**
	 * 数值分布范围
	 * 
	 * @return
	 */
	public LongRange getRange() {
		return this.range;
	}

	/**
	 * @param range
	 */
	public void setRange(LongRange range) {
		this.range = new LongRange(range);
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
		if (object == null || object.getClass() != LongZone.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((LongZone) object) == 0;
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
	public int compareTo(LongZone zone) {
		return this.range.compareTo(zone.range);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new LongZone(this);
	}
}