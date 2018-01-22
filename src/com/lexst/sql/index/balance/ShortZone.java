/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.index.balance;

import com.lexst.util.range.*;

/**
 * 短整型有效范围分布区域。<br>
 * 
 */
public class ShortZone extends IndexZone implements Comparable<ShortZone> {

	private static final long serialVersionUID = 2128281722073111552L;
	
	/** SHORT类型索引分布范围 **/
	private ShortRange range;

	/**
	 * 
	 * @param range
	 * @param count
	 */
	public ShortZone(ShortRange range, int count) {
		super();
		this.setRange(range);
		super.setWeight(count);
	}

	/**
	 * @param begin
	 * @param end
	 * @param count
	 */
	public ShortZone(short begin, short end, int count) {
		this(new ShortRange(begin, end), count);
	}

	/**
	 * @param zone
	 */
	public ShortZone(ShortZone zone) {
		super(zone);
		this.setRange(zone.range);
	}

	/**
	 * 数值分布范围
	 * 
	 * @return
	 */
	public ShortRange getRange() {
		return this.range;
	}

	/**
	 * @param range
	 */
	public void setRange(ShortRange range) {
		this.range = new ShortRange(range);
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
		if (object == null || object.getClass() != ShortZone.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((ShortZone) object) == 0;
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
	public int compareTo(ShortZone zone) {
		return this.range.compareTo(zone.range);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new ShortZone(this);
	}
}