/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * variable configure
 * 
 * @author scott.liang
 * 
 * @version 1.0 6/15/2009
 * @see com.lexst.sql.column.attribute
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.util.*;

/**
 * @author scott.liang
 *
 */
public abstract class VariableAttribute extends ColumnAttribute {
	
	private static final long serialVersionUID = 7629224218247560604L;

	/** 索引长度限制, 16个字符 **/
	public final static int INDEX_LIMIT = 16;
	
	/** 打包 **/
	protected Packing packing = new Packing();

	/** 默认值 (适用于RAW,CHAR,SCHAR,WCHAR) **/
	protected byte[] value;

	/** 最大索引长度(限可变长类型:RAW,CHAR,SCHAR,WCHAR) **/
	protected int indexSize;
	/** 默认索引  **/
	protected byte[] index;

	/**
	 * @param type
	 */
	public VariableAttribute(byte type) {
		super(type);
		this.setIndexSize(VariableAttribute.INDEX_LIMIT);
	}

	/**
	 * @param attribute
	 */
	public VariableAttribute(VariableAttribute attribute) {
		super(attribute);
		this.packing = new Packing(attribute.packing);
		this.indexSize = attribute.indexSize;
		this.setValue(attribute.value);
		this.setIndex(attribute.index);
	}

	/**
	 * 属性定义
	 * @param type
	 * @param columnId
	 * @param name
	 */
	public VariableAttribute(byte type, short columnId, String name) {
		this(type);
		this.setColumnId(columnId);
		this.setName(name);
	}
	
	/**
	 * 打包配置
	 * 
	 * @param compress
	 * @param encrypt
	 * @param password
	 */
	public void setPacking(int compress, int encrypt, byte[] password) {
		this.packing.setPacking(compress, encrypt, password);
	}

	/**
	 * 取打包对象
	 * @return
	 */
	public Packing getPacking() {
		return this.packing;
	}
	
	/**
	 * 最大索引长度
	 * 
	 * @param size
	 */
	public void setIndexSize(int size) {
		this.indexSize = size;
	}

	/**
	 * 最大索引长度
	 * 
	 * @return
	 */
	public int getIndexSize() {
		return this.indexSize;
	}

	/**
	 * set default value
	 * 
	 * @param b
	 */
	public boolean setValue(byte[] b) {
		return setValue(b, 0, (b == null ? 0 : b.length));
	}
	
	/**
	 * set default value
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public boolean setValue(byte[] b, int off, int len) {
		if (len < 1 || !super.isSetStatus()) return false;
		
		value = new byte[len];
		System.arraycopy(b, off, value, 0, len);

		this.setNull(false);

		return true;
	}

	/**
	 * get default value
	 * 
	 * @return
	 */
	public byte[] getValue() {
		return this.value;
	}

	/**
	 * set default index (from value)
	 * 
	 * @param b
	 */
	public void setIndex(byte[] b) {
		setIndex(b, 0, (b == null ? 0 : b.length));
	}
	
	/**
	 * set defalut index (from value)
	 * 
	 * @param b
	 * @param off
	 * @param len
	 */
	public void setIndex(byte[] b, int off, int len) {
		if (b == null || len == 0) {
			index = null;
		} else {
			index = new byte[len];
			System.arraycopy(b, off, index, 0, len);
		}
	}

	/**
	 * get default index
	 * 
	 * @return
	 */
	public byte[] getIndex() {
		return this.index;
	}
	
	/**
	 * 生成属性信息，保存到缓存中
	 * 
	 * @param buff
	 */
	protected void build(ByteArrayOutputStream buff) {
		// 数据类型
		buff.write(getType());
		// 列ID
		byte[] b = Numeric.toBytes(getColumnId());
		buff.write(b, 0, b.length);
		// 列名长度(最大64字节)
		b = getName().getBytes();
		buff.write((byte) (b.length & 0xFF));
		// 列名
		buff.write(b, 0, b.length);

		// 属性KEY类型
		buff.write(this.key);
		// 索引值长度限制
		b = Numeric.toBytes(this.indexSize);
		buff.write(b, 0, b.length);
		// 空状态 (允许或不允许)
		buff.write(super.buildNullable());

		// 打包域
		b = this.packing.build();
		buff.write(b, 0, b.length);
		
		// 默认函数
		this.buildFunction(buff);
		
		// 默认可变长值长度和值
		int size = (value == null ? 0 : value.length);
		b = Numeric.toBytes(size);
		buff.write(b, 0, b.length);
		if (size > 0) {
			buff.write(value, 0, size);
		}
		// 默认索引长度和索引
		size = (index == null ? 0 : index.length);
		b = Numeric.toBytes(size);
		buff.write(b, 0, b.length);
		if (size > 0) {
			buff.write(index, 0, size);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#build()
	 */
	@Override
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(128);
		this.build(buff);
		return buff.toByteArray();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.property.ColumnProperty#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		if(seek + 4 > end) {
			throw new ColumnAttributeResolveException("variable sizeout!");
		}
		// 数据类型
		setType(b[seek]);
		seek += 1;
		// 列ID
		setColumnId(Numeric.toShort(b, seek, 2));
		seek += 2;
		// 列名长度
		int size = b[seek] & 0xFF;
		seek += 1;
		// 列名
		if(seek + size > end) {
			throw new ColumnAttributeResolveException("variable sizeout!");
		}
		setName(b, seek, size);
		seek += size;
		
		if(seek + 4 > end) {
			throw new ColumnAttributeResolveException("variable sizeout!");
		}
		// 索引键 (主键，从键，未定义三种情况)
		this.key = b[seek];
		seek += 1;
		// 索引长度限制
		this.indexSize = Numeric.toInteger(b, seek, 4);
		seek += 4;
		// 空状态 (允许或者不)		
		super.resolveNullable(b[seek]);
		seek += 1;
				
		// 打包域
		size = packing.resolve(b, seek, end - seek);
		seek += size;
		
		// 默认函数
		size = this.resolveFunction(b, seek, end - seek);
		seek += size;

		// 默认值长度和变长值
		if(seek + 4 > end) {
			throw new ColumnAttributeResolveException("value sizeout!");
		}
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (size > 0) {
			if(seek + size > end) {
				throw new ColumnAttributeResolveException("value sizeout!");
			}
			this.setValue(b, seek, size);
			seek += size;
		}

		// 默认索引长度和索引
		if(seek + 4 > end) {
			throw new ColumnAttributeResolveException("index sizeout!");
		}
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (size > 0) {
			if(seek + size > end) {
				throw new ColumnAttributeResolveException("index sizeout!");
			}
			this.setIndex(b, seek, size);
			seek += size;
		}

		return seek - off;
	}

}