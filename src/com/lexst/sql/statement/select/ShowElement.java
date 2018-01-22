/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.select;

import java.io.*;

/**
 * @author scott.liang
 * 
 */
public abstract class ShowElement implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public final static byte COLUMN = 1;
	public final static byte FUNCTION = 2;

	/** 列成员或者函数成员  */
	protected byte kind;
	
	/** 别名(用户临时定义名称) */
	protected String alias;

	/**
	 * default
	 */
	protected ShowElement() {
		super();
	}

	/**
	 * 
	 * @param kind
	 */
	protected ShowElement(byte kind) {
		this();
		this.setKind(kind);
	}

	public void setKind(byte b) {
		this.kind = b;
	}

	public byte getKind() {
		return this.kind;
	}
	
	public boolean isColumn() {
		return this.kind == ShowElement.COLUMN;
	}
	
	public boolean isFunction() {
		return this.kind == ShowElement.FUNCTION;
	}

	public void setAlias(String s) {
		this.alias = s;
	}

	public String getAlias() {
		return this.alias;
	}
	
	/**
	 * 返回实际的列ID号(对应Table中的属性ID号)
	 * @return
	 */
	public abstract short getColumnId();

	/**
	 * column identity or function identity
	 * 
	 * @return
	 */
	public abstract short getIdentity();
	
	public abstract byte getType();
	
	public abstract String getName();
	
}