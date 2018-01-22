/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function;

import java.io.*;

import com.lexst.sql.column.*;

/**
 * @author scott.liang
 * 
 */
public abstract class SQLValue implements Serializable, java.lang.Comparable<SQLValue> {

	private static final long serialVersionUID = 1L;

	public final static int ROWSET = 0x1;
	
	public final static int RAW = 0x10;
	public final static int STRING = 0x11;
	public final static int SHORT = 0x12;
	public final static int INTEGER = 0x13;
	public final static int LONG = 0x14;
	public final static int FLOAT = 0x15;
	public final static int DOUBLE = 0x16;
	public final static int DATE = 0x17;
	public final static int TIME = 0x18;
	public final static int TIMESTAMP = 0x19;

	/** SQL参数的数据类型 **/
	private int type;
	
	/**
	 * 
	 * @param type
	 */
	protected SQLValue(int type) {
		super();
		this.setType(type);
	}
	
	public int getType() {
		return this.type;
	}

	public void setType(int i) {
		this.type = i;
	}

	public boolean isRowset() {
		return type == SQLValue.ROWSET;
	}

	public boolean isRaw() {
		return type == SQLValue.RAW;
	}

	public boolean isString() {
		return type == SQLValue.STRING;
	}

	public boolean isShort() {
		return type == SQLValue.SHORT;
	}

	public boolean isInteger() {
		return type == SQLValue.INTEGER;
	}

	public boolean isLong() {
		return type == SQLValue.LONG;
	}

	public boolean isFloat() {
		return type == SQLValue.FLOAT;
	}

	public boolean isDouble() {
		return type == SQLValue.DOUBLE;
	}

	public boolean isDate() {
		return type == SQLValue.DATE;
	}

	public boolean isTime() {
		return type == SQLValue.TIME;
	}

	public boolean isTimestamp() {
		return type == SQLValue.TIMESTAMP;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public SQLValue clone() {
		return duplicate();
	}
	
	/*
	 * 比较两组参数
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(SQLValue param) {
		if(param.getType() != this.getType()) {
			throw new IllegalArgumentException("not match type!");
		}
		// 去子类中比较
		return compareIt(param);
	}

	/**
	 * 复制对象
	 * 
	 * @return
	 */
	public abstract SQLValue duplicate();
	
	/**
	 * 由子类实现，去具体比较两组参数
	 * @param param
	 * @return
	 */
	public abstract int compareIt(SQLValue param);

	/**
	 * 根据列标识号，生成列
	 * @param columnId
	 * @return
	 */
	public abstract Column toColumn(short columnId);
	
	// compateIt

//	public abstract boolean equal(SQLValue result);
//	
//	public abstract boolean notEqual(SQLValue result);
//	
//	public abstract boolean less(SQLValue result);
//	
//	public abstract boolean lessEqual(SQLValue result);
//	
//	public abstract boolean greater(SQLValue result);
//	
//	public abstract boolean greaterEqual(SQLValue value);
	
//	public boolean like(SQLValue result);

//	public final static byte GREATER = 5;
//	public final static byte GREATER_EQUAL = 6;
//	public final static byte LIKE = 7;

}