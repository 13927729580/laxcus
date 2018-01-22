/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function;

import com.lexst.sql.schema.*;

/**
 * @author scott.liang
 *
 */
public class Format extends SQLFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * default
	 */
	public Format() {
		super();
	}

	/**
	 * @param def
	 */
	public Format(Format def) {
		super(def);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#duplicate()
	 */
	@Override
	public SQLFunction duplicate() {
		return new Format(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#compute(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public SQLValue compute(SQLValue value)  {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#create(com.lexst.sql.schema.Table, java.lang.String)
	 */
	@Override
	public SQLFunction create(Table table, String sqlText) {
		// TODO Auto-generated method stub
		return null;
	}

}
