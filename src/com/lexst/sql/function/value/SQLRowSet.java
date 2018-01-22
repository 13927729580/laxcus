/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function.value;

import java.util.*;

import com.lexst.sql.column.*;
import com.lexst.sql.function.*;
import com.lexst.sql.row.*;

/**
 * @author scott.liang
 *
 */
public class SQLRowSet extends SQLValue {
	
	private static final long serialVersionUID = 1L;
	
	private List<Row> array;

	/**
	 * default
	 */
	public SQLRowSet() {
		super(SQLValue.ROWSET);
	}
	
	/**
	 * @param param
	 */
	public SQLRowSet(SQLRowSet param) {
		this();
		this.array = new ArrayList<Row>(param.array);
	}
	
	/**
	 * @param rows
	 */
	public SQLRowSet(List<Row> rows) {
		this();
		this.setValue(rows);
	}

	public void setValue(List<Row> rows) {
		this.array = rows;
	}
	
	public List<Row> getValue() {
		return this.array;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLRowSet(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLRowSet sht = (SQLRowSet)param;
		return -1;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		// TODO Auto-generated method stub
		return null;
	}
	

}