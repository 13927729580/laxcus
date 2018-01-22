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
public class CharBalancer extends Bit32Balancer {

	private static final long serialVersionUID = 1L;

	/**
	 * default
	 */
	public CharBalancer() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.Bit32Balancer#getSector()
	 */
	@Override
	public Bit32Sector getSector() {
		return new CharSector();
	}

}