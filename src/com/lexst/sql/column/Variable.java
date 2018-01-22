/**
 * 
 */
package com.lexst.sql.column;

import java.io.*;
import java.util.*;

import com.lexst.log.client.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.util.*;
import com.lexst.util.*;


public abstract class Variable extends Column {

	private static final long serialVersionUID = 1L;

	/* origin value, from user*/
	protected byte[] value;
	
	/* index value, from value */
	protected byte[] index;

	/* hash code */
	protected int hash;

	/**
	 * default
	 */
	protected Variable() {
		super();
	}

	/**
	 * @param type
	 */
	protected Variable(byte type) {
		super(type);
	}

	/**
	 * @param variable
	 */
	public Variable(Variable variable) {
		super(variable);
		this.setValue(variable.value);
		this.setIndex(variable.index);
		this.hash = variable.hash;
	}

	/**
	 * set binary data
	 * @param b
	 */
	public void setValue(byte[] b) {
		if (b == null) {
			this.setValue(null, 0, 0);
		} else {
			this.setValue(b, 0, b.length);
		}
	}

	/**
	 * set binary data
	 * 
	 * @param b
	 * @param off
	 * @param len
	 */
	public void setValue(byte[] b, int off, int len) {
		if (b == null) {
			value = null;
		} else {
			if (len < 0) {
				throw new IllegalArgumentException("illegal value size:" + len);
			}
			value = new byte[len];
			System.arraycopy(b, off, value, 0, len);
		}
		setNull(value == null);
	}
	
	/**
	 * 返回值(可能是加密、压缩状态)
	 * @return
	 */
	public byte[] getValue() {
		return this.value;
	}

	/**
	 * 返回原始数值(已经解密和解压缩)
	 * @param packing
	 * @return
	 */
	public byte[] getValue(Packing packing) {
		if (value != null && value.length > 0 && packing != null && packing.isEnabled()) {
			try {
				return VariableGenerator.depacking(packing, value, 0, value.length);
			} catch (IOException e) {
				Logger.error(e);
				return null;
			}
		}
		return value;
	}

	/**
	 * set low bytes
	 * 
	 * @param b
	 */
	public void setIndex(byte[] b) {
		if (b == null) {
			this.setIndex(null, 0, 0);
		} else {
			this.setIndex(b, 0, b.length);
		}
	}

	/**
	 * set low bytes
	 * 
	 * @param b
	 * @param off
	 * @param len
	 */
	public void setIndex(byte[] b, int off, int len) {
		if (b == null) {
			index = null;
		} else {
			if (len < 0) throw new IllegalArgumentException("illegal index size:" + len);
			index = new byte[len];
			System.arraycopy(b, off, index, 0, len);
		}
	}

	/**
	 * get low bytes
	 * 
	 * @return
	 */
	public byte[] getIndex() {
		return this.index;
	}
	
	/**
	 * 返回有效数据<br>
	 * 如果索引存在即返回索引，否则返回值
	 * 
	 * @return
	 */
	public byte[] getValid() {
		if (index != null) return index;
		return value;
	}

	/**
	 * compare  equals
	 * @param var
	 * @return
	 */
	protected boolean equals(Variable var) {
		if (!isNull() && !var.isNull()) {
			if (index != null) {
				if (index.length != var.index.length) return false;
				for (int i = 0; i < index.length; i++) {
					if (index[i] != var.index[i]) return false;
				}
			} else {
				if (value.length != var.value.length) return false;
				for (int i = 0; i < value.length; i++) {
					if (value[i] != var.value[i]) return false;
				}
			}
			return true;
		}
		
		return isNull() == var.isNull();
	}

	/*
	 * generate hash code
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		if(isNull()) return 0;
		
		if (this.hash == 0) {
			if (index != null && index.length > 0) {
				this.hash = Arrays.hashCode(index);
			} else if (value != null && value.length > 0) {
				this.hash = Arrays.hashCode(value);
			}
		}
		return this.hash;
	}
	
	/*
	 * 按照字节大小比较
	 * @see com.lexst.sql.column.Column#compare(com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column column) {
		if (isNull() && column.isNull()) return 0;
		else if (isNull()) return -1;
		else if (column.isNull()) return 1;

		Variable var = (Variable) column;
		
		// 如果索引存在，比较索引；否则比较数值
		if (index != null && index.length > 0) {
			if (var.index == null || var.index.length == 0) return 1;
			int len = (index.length < var.index.length ? index.length : var.index.length);

			for (int i = 0; i < len; i++) {
				if (index[i] != var.index[i]) {
					return index[i] < var.index[i] ? -1 : 1;
				}
			}
			return (index.length < var.index.length ? -1 : (index.length > var.index.length ? 1 : 0));
		} else {
			if (value == null || value.length == 0) return -1;
			if (var.value == null || var.value.length == 0) return 1;

			int len = (value.length < var.value.length ? value.length : var.value.length);

			for (int i = 0; i < len; i++) {
				if (value[i] != var.value[i]) {
					return value[i] < var.value[i] ? -1 : 1;
				}
			}
			return (value.length < var.value.length ? -1 : (value.length > var.value.length ? 1 : 0));
		}
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Column#capacity()
	 */
	@Override
	public int capacity() {
		if (isNull()) return 1;
		int size = 5;
		size += (index == null ? 0 : index.length);
		return size + (value == null ? 0 : value.length);
	}
	
	/*
	 * build data to stream
	 * @see com.lexst.sql.column.Column#build(java.io.ByteArrayOutputStream)
	 */
	public int build(ByteArrayOutputStream stream) {
		byte tag = build_tag();
		stream.write(tag);

		if (isNull()) return 1;

		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		
		// write value (size + content) 
		byte[] b = Numeric.toBytes(value.length);
		buff.write(b, 0, b.length);
		buff.write(value, 0, value.length);
		
		// scan index and write it
		int len = (index == null ? 0 : index.length);
		if (len > 0) {
			b = Numeric.toBytes(len);
			buff.write(b, 0, b.length);
			buff.write(index, 0, index.length);
		}
		
		// write body
		byte[] data = buff.toByteArray();
		int allsize = 4 + data.length;
		b = Numeric.toBytes(allsize);
		
		stream.write(b, 0, b.length);
		stream.write(data, 0, data.length);
		
		return 1 + allsize;
	}
	
	/*
	 * resolve parameter from stream
	 * @see com.lexst.sql.column.Column#resolve(byte[], int, int)
	 */
	public int resolve(byte[] b, int off, int len) {
		int end = off + len;
		int seek = off;
		
		// resolve tag
		resolve_tag(b[seek++]);

		// null status, exit
		if (isNull()) {
			return seek - off;
		}
		
		// all size
		if(seek + 4 > end){
			throw new SizeOutOfBoundsException("body sizeout!");
		}
		int allsize = Numeric.toInteger(b, seek, 4);
		if (seek + allsize > end) {
			throw new SizeOutOfBoundsException("body sizeout!");
		}
		seek += 4;

		// 值域
		if (seek + 4 > end) {
			throw new SizeOutOfBoundsException("value sizeout!");
		}
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (seek + size > end) {
			throw new SizeOutOfBoundsException("value sizeout!");
		}
		this.setValue(b, seek, size);
		seek += size;
		if (off + 1 + allsize == seek) {
			return seek - off;
		}
		
		// 索引域
		if (seek + 4 > end) {
			throw new SizeOutOfBoundsException("index sizeout!");
		}
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if(size > 0) {
			if (seek + size > end) {
				throw new SizeOutOfBoundsException("index sizeout!");
			}
			this.setIndex(b, seek, size);
			seek += size;
		}

		return seek - off;
	}

}