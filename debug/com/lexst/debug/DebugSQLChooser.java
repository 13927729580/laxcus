/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.debug;

import com.lexst.sql.parse.*;
import com.lexst.sql.schema.*;

/**
 * @author scott.liang
 *
 */
public class DebugSQLChooser implements SQLChooser {

	private Table table;
	
	/**
	 * 
	 */
	public DebugSQLChooser() {
		// TODO Auto-generated constructor stub
	}
	
	public void setTable(Table s) {
		this.table = s;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.parse.SQLChooser#findTable(com.lexst.sql.schema.Space)
	 */
	@Override
	public Table findTable(Space space) {
		// TODO Auto-generated method stub
		return this.table;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.parse.SQLChooser#onTable(com.lexst.sql.schema.Space)
	 */
	@Override
	public boolean onTable(Space space) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.parse.SQLChooser#onSchema(java.lang.String)
	 */
	@Override
	public boolean onSchema(String schema) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.parse.SQLChooser#onUser(java.lang.String)
	 */
	@Override
	public boolean onUser(String username) {
		// TODO Auto-generated method stub
		return false;
	}

}
