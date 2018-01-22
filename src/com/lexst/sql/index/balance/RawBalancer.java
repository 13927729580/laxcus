/**
 * @email admin@laxcus.com
 */
package com.lexst.sql.index.balance;

import com.lexst.sql.index.section.*;

/**
 * 二进制数据流平衡分布器。<br>
 * 
 */
public class RawBalancer extends Bit64Balancer {

	private static final long serialVersionUID = 4761532798715203848L;

	/**
	 * default
	 */
	public RawBalancer() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.balance.Bit64Balancer#getSector()
	 */
	@Override
	public Bit64Sector getSector() {
		return new LongSector();
	}

}