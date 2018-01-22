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
public class FloatZone extends IndexZone implements Comparable<FloatZone> {

	private static final long serialVersionUID = 6767216664196614004L;
	
	/** 数据值分布范围 **/
	private FloatRange range;

	/**
	 * 
	 * @param range
	 * @param count
	 */
	public FloatZone(FloatRange range, int count) {
		super();
		this.setRange(range);
		super.setWeight(count);
	}

	/**
	 * @param begin
	 * @param end
	 * @param count
	 */
	public FloatZone(float begin, float end, int count) {
		this(new FloatRange(begin, end), count);
	}

	/**
	 * @param zone
	 */
	public FloatZone(FloatZone zone) {
		super(zone);
		this.setRange(zone.range);
	}

	/**
	 * 数值分布范围
	 * 
	 * @return
	 */
	public FloatRange getRange() {
		return this.range;
	}

	/**
	 * @param range
	 */
	public void setRange(FloatRange range) {
		this.range = new FloatRange(range);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s %d", this.range, super.getWeight());
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != FloatZone.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((FloatZone) object) == 0;
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
	public int compareTo(FloatZone zone) {
		return this.range.compareTo(zone.range);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new FloatZone(this);
	}
}