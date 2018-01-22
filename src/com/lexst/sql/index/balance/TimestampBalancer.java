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
public class TimestampBalancer extends Bit64Balancer {

	private static final long serialVersionUID = 4125854369700290516L;

	/**
	 * 
	 */
	public TimestampBalancer() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.Bit64Balancer#getSector()
	 */
	@Override
	public Bit64Sector getSector() {
		return new TimestampSector();
	}

}
