/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function;

import java.util.regex.*;

import com.lexst.sql.function.value.*;
import com.lexst.sql.schema.*;

/**
 * @author scott.liang
 * 
 */
public class Now extends SQLFunction {

	private static final long serialVersionUID = 1L;

	private final static String regex = "^\\s*(?i)NOW\\s*\\(\\s*\\)\\s*$";
	
	/**
	 * default
	 */
	public Now() {
		super();
	}

	/**
	 * @param now
	 */
	public Now(Now now) {
		super(now);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#compute(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public SQLValue compute(SQLValue value)  {
		java.util.Date date = new java.util.Date(System.currentTimeMillis());
		return new SQLTimestamp(date);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#duplicate()
	 */
	@Override
	public SQLFunction duplicate() {
		return new Now(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#create(com.lexst.sql.schema.Table, java.lang.String)
	 */
	@Override
	public SQLFunction create(Table table, String sqlText) {
		Pattern pattern = Pattern.compile(Now.regex);
		Matcher matcher = pattern.matcher(sqlText);
		if (matcher.matches()) {
			return new Now();
		}
		return null;
	}

}