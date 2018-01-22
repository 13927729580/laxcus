/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.balance;

import com.lexst.sql.index.section.*;

/**
 * 整型区域平衡分割器。<br>
 *
 */
public class IntegerBalancer extends Bit32Balancer {

	private static final long serialVersionUID = -3303110475768512093L;

	/**
	 * default
	 */
	public IntegerBalancer() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.balance.Bit32Balancer#getSector()
	 */
	@Override
	public Bit32Sector getSector() {
		return new IntegerSector();
	}

}
