/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.balance;

import com.lexst.sql.index.section.*;

/**
 * @author scott.liang
 *
 */
public class LongBalancer extends Bit64Balancer {

	private static final long serialVersionUID = 1823632302579193193L;

	/**
	 * default
	 */
	public LongBalancer() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.Bit64Balancer#getSector()
	 */
	@Override
	public Bit64Sector getSector() {
		return new LongSector();
	}

}
