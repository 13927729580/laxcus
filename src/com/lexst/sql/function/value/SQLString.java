/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function.value;

import java.io.*;

import com.lexst.log.client.*;
import com.lexst.sql.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.*;
import com.lexst.sql.util.*;

/**
 * @author scott.liang
 *
 */
public class SQLString extends SQLValue {
	
	private static final long serialVersionUID = 1L;
	
	private short left, right;
	protected boolean sentient;
	private String value;
	
	private byte originType;
	
	/**
	 * default
	 */
	public SQLString() {
		super(SQLValue.STRING);
		left = right = 0;
		this.setSentient(true);
	}

	/**
	 * @param type
	 * @param text
	 */
	public SQLString(String text) {
		this();
		this.setValue(text);
	}

	/**
	 * @param left
	 * @param right
	 * @param type
	 * @param text
	 */
	public SQLString(short left, short right, String text) {
		this();
		this.setValue(text);
		this.setRange(left, right);
	}

	/**
	 * @param param
	 */
	public SQLString(SQLString param) {
		this();
		this.left = param.left;
		this.right = param.right;
		this.sentient = param.sentient;
		this.value = param.value;
	}
	
	/**
	 * 大小写敏感 (CASE or NOTCASE)
	 * 
	 * @param b
	 */
	public void setSentient(boolean b) {
		this.sentient = b;
	}

	/**
	 * 是否大小写敏感
	 * 
	 * @return
	 */
	public boolean isSentient() {
		return this.sentient;
	}
	
	public void setOriginType(byte b) {
		this.originType = b;
	}
	public byte getOriginType() {
		return this.originType;
	}
	
	public void setRange(short left, short right) {
		this.left = left;
		this.right = right;
	}
	
	public short getLeft() {
		return this.left;
	}
	
	public short getRight() {
		return this.right;
	}

	public void setValue(String s) {
		this.value = s;
	}
	
	public String getValue() {
		return this.value;
	}
	
	
	/**
	 * LIKE 比较是否匹配
	 * @param string
	 * @return
	 */
	public boolean isLike(SQLString string) {
		if (left == 0 && right == 0) {
			if (!this.isSentient() || !string.isSentient()) {
				return value.toLowerCase().indexOf(string.value.toLowerCase()) > -1;
			} else {
				return value.indexOf(string.value) > -1;
			}
		} else {

		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLString(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLString string = (SQLString) param;
		// 大小写不敏感
		if (!isSentient() || !string.isSentient()) {
			return value.compareToIgnoreCase(string.value);
		} else {
			return value.compareTo(string.value);
		}
	}
	
	public Column toColumn(short columnId, Packing packing) {
		try {
			switch (this.originType) {
			case Type.CHAR:
				byte[] b = new UTF8().encode(value);
				b = VariableGenerator.enpacking(packing, b, 0, b.length);
				return new Char(columnId, b);
			case Type.SCHAR:
				b = new UTF16().encode(value);
				b = VariableGenerator.enpacking(packing, b, 0, b.length);
				return new SChar(columnId, b);
			case Type.WCHAR:
				b = new UTF32().encode(value);
				b = VariableGenerator.enpacking(packing, b, 0, b.length);
				return new WChar(columnId, b);
			}
		} catch (IOException e) {
			Logger.error(e);
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		switch (this.originType) {
		case Type.SCHAR:
			return new SChar(columnId, new UTF16().encode(value));
		case Type.WCHAR:
			return new WChar(columnId, new UTF32().encode(value));
		default:
			return new Char(columnId, new UTF8().encode(value));
		}
	}

}