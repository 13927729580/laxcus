/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.sort;

import java.io.*;

import com.lexst.log.client.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.util.*;

/**
 * @author scott.liang
 *
 */
public abstract class WordComparator implements ColumnComparator {
	
	/** 列标识号 **/
	protected short columnId;
	
	/** 字符大小写是否敏感(CASE or NOT CASE)。默认是敏感(TRUE) **/
	protected boolean sentient;
	/** 数据打包接口 **/
	protected Packing packing = new Packing();
	
	/** 对应的字符集 **/
	protected Charset charset;

	/**
	 * set charset
	 */
	protected WordComparator(Charset cs) {
		super();
		this.sentient = true;
		this.charset = cs;
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
	
	/**
	 * 设置数据打包参数
	 * @param p
	 */
	public void setPacking(Packing p) {
		this.packing.set(p);
	}
	
	/**
	 * 返回数据打包参数
	 * @return
	 */
	public Packing getPacking() {
		return this.packing;
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnComparator#getColumnId()
	 */
	@Override
	public short getColumnId() {
		return this.columnId;
	}
	
	public void setColumnId(short id) {
		this.columnId = id;
	}

	/**
	 * charset instance
	 * @return
	 */
	public Charset getCharset() {
		return this.charset;
	}

	/**
	 * 
	 * @param b1
	 * @param b2
	 * @return
	 */
	protected int compare(byte[] b1, byte[] b2) {
		// 如果数据被打包(压缩和加密，执行反操作)
		if(packing.isEnabled()) {
			try {
				b1 = VariableGenerator.depacking(packing, b1, 0, b1.length);
				b2 = VariableGenerator.depacking(packing, b2, 0, b2.length);
			} catch(IOException e) {
				Logger.error(e);
				return -1;
			}
		}
		// 解码，转成字符串
		String s1 = charset.decode(b1, 0, b1.length);
		String s2 = charset.decode(b2, 0, b2.length);
		// 大小写敏感
		if(this.sentient) {
			return s1.compareTo(s2);
		} else {
			// 忽略大小写进行比较
			return s1.compareToIgnoreCase(s2);
		}
	}

}