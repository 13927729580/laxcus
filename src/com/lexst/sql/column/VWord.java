/**
 * 
 */
package com.lexst.sql.column;

import java.io.*;
import java.util.*;

import com.lexst.util.*;

/**
 * Vague Word, sub class: VChar, VSChar, VWChar
 * 
 * @author scott.liang
 *
 */
public abstract class VWord extends Column {

	private static final long serialVersionUID = 1L;

	/* ignore size */
	private short left, right;
	
	/* search value */
	private byte[] index;
	
	/* message degist */
	private int hash;
	
	/**
	 *  default
	 */
	protected VWord() {
		super();
	}

	/**
	 * @param column
	 */
	public VWord(VWord column) {
		super(column);
		this.left = column.left;
		this.right = column.right;
		this.setIndex(column.index);
		this.hash = column.hash;
	}

	/**
	 * @param type
	 */
	public VWord(byte type) {
		super(type);
	}

	/**
	 * @param type
	 * @param id
	 */
	public VWord(byte type, short id) {
		super(type, id);
	}

	/**
	 * set binary data
	 * @param b
	 */
	public void setIndex(byte[] b) {
		if (b == null || b.length == 0) {
			this.setIndex(null, 0, 0);
		} else {
			this.setIndex(b, 0, b.length);
		}
	}
	
	/**
	 * set index data
	 * @param b
	 * @param off
	 * @param len
	 */
	public void setIndex(byte[] b, int off, int len) {
		if (b == null || b.length == 0 || len < 0) {
			index = null;
		} else {
			index = new byte[len];
			System.arraycopy(b, off, index, 0, len);
		}
		setNull(index == null);
	}
	
	/**
	 * get origin binary
	 * @return
	 */
	public byte[] getIndex() {
		return this.index;
	}

	/**
	 * ignore size of left
	 * @param i
	 */
	public void setLeft(short i) {
		this.left = i;
	}
	/**
	 * get left size
	 * @return
	 */
	public int getLeft() {
		return this.left;
	}

	/**
	 * ignore size of right
	 * @param i
	 */
	public void setRight(short i) {
		this.right = i;
	}
	
	/**
	 * get right size
	 * @return
	 */
	public short getRight() {
		return this.right;
	}
	
	/**
	 * set ignore range
	 * @param left
	 * @param right
	 */
	public void setRange(short left, short right) {
		this.setLeft(left);
		this.setRight(right);
	}

	/**
	 * compare word equals
	 * @param w
	 * @return
	 */
	protected boolean equals(VWord w) {
		if (this.isNull() && w.isNull()) return true;

		if (left == w.left && right == w.right) {
			if (w.index == null || index.length != w.index.length) return false;
			for (int i = 0; i < index.length; i++) {
				if (index[i] != w.index[i]) return false;
			}
			return true;
		}
		return false;
	}

	/*
	 * generate message degist code
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		if (hash == 0) {
			if (index != null && index.length > 0) {
				hash = Arrays.hashCode(index);
			}
		}
		return hash ^ left ^ right;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.column.Column#compare(com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column column) {
		if (isNull() && column.isNull()) return 0;
		else if (isNull()) return -1;
		else if (column.isNull()) return 1;
		
		VWord w = (VWord)column;
		int len = (index.length < w.index.length ? index.length : w.index.length);
		for(int i = 0; i < len; i++) {
			if(index[i] < w.index[i]) {
				return index[i] < w.index[i] ? -1 : 1;
			}
		}
		return (index.length < w.index.length ? -1 : (index.length > w.index.length ? 1 : 0));
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#capacity()
	 */
	@Override
	public int capacity() {
		if (isNull()) return 1;
		return 9 + (index == null ? 0 : index.length);
	}

	/*
	 * build vague word to stream
	 * @see com.lexst.sql.column.Column#build(java.io.ByteArrayOutputStream)
	 */
	public int build(ByteArrayOutputStream stream) {
		byte tag = build_tag();
		stream.write(tag);

		if (isNull()) return 1;

		int size = (index == null ? 0 : index.length);
		int allsize = 8 + size;
		
		byte[] b = Numeric.toBytes(allsize);
		stream.write(b, 0, b.length);
		// write left
		b = Numeric.toBytes(left);
		stream.write(b, 0, b.length);
		// write right
		b = Numeric.toBytes(right);
		stream.write(b, 0, b.length);
		// write index
		if (size > 0) {
			stream.write(index, 0, index.length);
		}

		return allsize;
	}

	/*
	 * resolve vague word from stream
	 * @see com.lexst.sql.column.Column#resolve(byte[], int, int)
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		resolve_tag(b[seek++]);
		if (isNull()) return 1;
		
		if (seek + 8 > end) {
			throw new SizeOutOfBoundsException("vword sizeout!");
		}
		
		// scan all size
		int allsize = Numeric.toInteger(b, seek, 4);
		if(seek + allsize > end) {
			throw new SizeOutOfBoundsException("vword sizeout!");
		}
		seek += 4;
		// scan left and right
		this.left = Numeric.toShort(b, seek, 2);
		seek += 2;
		this.right = Numeric.toShort(b, seek, 2);
		seek += 2;
		// scan index
		int size = allsize - 8;
		if (size > 0) {
			this.setIndex(b, seek, size);
			seek += size;			
		}

		return seek - off;
	}

}