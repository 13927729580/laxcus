/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function.value;

import java.io.*;

import com.lexst.log.client.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.*;
import com.lexst.sql.util.*;

/**
 * @author scott.liang
 * 
 */
public class SQLRaw extends SQLValue {

	private static final long serialVersionUID = 1L;

	private byte[] value;

	/**
	 * default
	 */
	public SQLRaw() {
		super(SQLValue.RAW);
	}

	/**
	 * @param obj
	 */
	public SQLRaw(SQLRaw obj) {
		this();
		this.setValue(obj.value, 0, obj.value.length);
	}

	/**
	 * @param b
	 * @param off
	 * @param len
	 */
	public SQLRaw(byte[] b, int off, int len) {
		this();
		this.setValue(b, off, len);
	}

	/**
	 * save value
	 * 
	 * @param b
	 * @param off
	 * @param len
	 */
	public void setValue(byte[] b, int off, int len) {
		if (b == null) {
			value = null;
		} else {
			value = new byte[len];
			System.arraycopy(b, off, value, 0, value.length);
		}

	}

	public byte[] getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLRaw(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLRaw sht = (SQLRaw)param;
		return -1;
	}
	
	/**
	 * @param columnId
	 * @param packing
	 * @return
	 */
	public Column toColumn(short columnId, Packing packing) {
		if(packing != null && packing.isEnabled()) {
			byte[] b = null;
			try {
				b = VariableGenerator.enpacking(packing, value, 0, value.length);
			} catch(IOException e) {
				Logger.error(e);
			}
			return new com.lexst.sql.column.Raw(columnId, b);
		} else {
			return new com.lexst.sql.column.Raw(columnId, value);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		return toColumn(columnId, null);
	}
	
}