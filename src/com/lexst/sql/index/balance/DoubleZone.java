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
public class DoubleZone extends IndexZone implements Comparable<DoubleZone> {

	private static final long serialVersionUID = 5304664024947981771L;

	/** 数据值分布范围 **/
	private DoubleRange range;

	/**
	 * 
	 * @param range
	 * @param count
	 */
	public DoubleZone(DoubleRange range, int count) {
		super();
		this.setRange(range);
		super.setWeight(count);
	}

	/**
	 * @param begin
	 * @param end
	 * @param count
	 */
	public DoubleZone(double begin, double end, int count) {
		this(new DoubleRange(begin, end), count);
	}

	/**
	 * @param zone
	 */
	public DoubleZone(DoubleZone zone) {
		super(zone);
		this.setRange(zone.range);
	}

	/**
	 * 数值分布范围
	 * 
	 * @return
	 */
	public DoubleRange getRange() {
		return this.range;
	}

	/**
	 * @param range
	 */
	public void setRange(DoubleRange range) {
		this.range = new DoubleRange(range);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s %d", range, super.getWeight());
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != DoubleZone.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((DoubleZone) object) == 0;
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
	public int compareTo(DoubleZone zone) {
		return this.range.compareTo(zone.range);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new DoubleZone(this);
	}
}