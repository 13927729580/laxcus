/**
 * 
 */
package com.lexst.sql.column;

import java.io.*;
import java.util.*;

import com.lexst.sql.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.util.*;

public abstract class Word extends Variable {

	private static final long serialVersionUID = 1L;
	
	/* LIKE关键字 */
	private List<VWord> array = new ArrayList<VWord>();
	
	/**
	 * @param type
	 */
	protected Word(byte type) {
		super(type);
	}
	
	/**
	 * clone 
	 * @param word
	 */
	protected Word(Word word) {
		super(word);
		this.array.addAll(word.array);
	}
	
	/**
	 * save a vword
	 * @param tag
	 */
	public void addVWord(VWord tag) {
		array.add(tag);
	}
	
	/**
	 * save vword set
	 * @param tag
	 */
	public void addVWords(Collection<VWord> tag) {
		array.addAll(tag);
	}

	/**
	 * 字节解码，返回字符串格式
	 * 
	 * @param packing - 打包配置(压缩和加密)
	 * @param charset - 编码格式
	 * @param limit - 字节长度限制
	 * @return
	 */
	public String toString(Packing packing, Charset charset, int limit) {
		if (isNull()) return "NULL";

		// 返回初始数据流
		byte[] b = super.getValue(packing);
		// 解码
		String s = charset.decode(b, 0, b.length);
		// 如果限制长度，返回指定长度
		if (limit > 0 && limit < s.length()) {
			return s.substring(0, limit);
		}
		return s;
	}

	/**
	 * 比较两个列是否一致
	 * @param word
	 * @return
	 */
	protected boolean equals(Word word) {
		if (this.isNull() && word.isNull()) return true;
		else if(isNull() || word.isNull()) return false;

		// 如果双方都有索引，以索引做比较
		if (index != null && index.length > 0) {
			if (index.length != word.index.length) return false;
			for (int i = 0; i < index.length; i++) {
				if (index[i] != word.index[i]) return false;
			}
		} else {
			// 否则，以值做比较
			if (value.length != word.value.length) return false;
			for (int i = 0; i < value.length; i++) {
				if (value[i] != word.value[i]) return false;
			}
		}
		return true;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Variable#hashCode()
	 */
	@Override
	public int hashCode() {
		if(isNull()) return 0;
		
		// 如果"值"域在属性定义时忽略大小写，如果解决?
		if (this.hash == 0) {
			if (index != null && index.length > 0) {
				this.hash = Arrays.hashCode(index);
			} else if (value != null && value.length > 0) {
				this.hash = Arrays.hashCode(value);
			}
		}
		return this.hash;
	}
	
	/**
	 * 按照字符串编码比较
	 * 
	 * @param charset
	 * @param column
	 * @return
	 */
	protected int compare(Charset charset, Column column) {
		if (isNull() && column.isNull()) return 0;
		else if (isNull()) return -1;
		else if (column.isNull()) return 1;
		else if (this.getType() != column.getType()) {
			return super.compareTo(column); // 按照ID排列比较
		}
		
		Word word = (Word) column;
		
		if (index != null && word.index != null) {
			String s1 = charset.decode(index, 0, index.length);
			String s2 = charset.decode(word.index, 0, word.index.length);
			return s1.compareTo(s2);
		} else {
			if (value == null || value.length == 0) return -1;
			if (word.value == null || word.value.length == 0) return 1;

			String s1 = charset.decode(value, 0, value.length);
			String s2 = charset.decode(word.value, 0, word.value.length);
			return s1.compareTo(s2);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Variable#capacity()
	 */
	@Override
	public int capacity() {
		if (isNull()) return 1;

		int size = 5;
		size += (index == null ? 0 : index.length);
		size += (value == null ? 0 : value.length);
		for (VWord vword : array) {
			size += vword.capacity();
		}
		return size;
	}
	
	/*
	 * build word to stream
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
		if(value.length > 0) buff.write(value, 0, value.length);
		
		// scan index and like set
		int indexLen = (index == null ? 0 : index.length);
		int count = array.size();
		if (indexLen > 0 || count > 0) {
			// index field 
			b = Numeric.toBytes(indexLen);
			buff.write(b, 0, b.length);
			if(indexLen > 0) {
				buff.write(index, 0, index.length);
			}

			// like field
			ByteArrayOutputStream likes = new ByteArrayOutputStream();
			for(VWord like : array){
				like.build(likes);
			}
			// like count
			b = Numeric.toBytes(count);
			buff.write(b, 0, b.length);
			// like content
			if (count > 0) {
				b = likes.toByteArray();
				buff.write(b, 0, b.length);
			}
		}
		
		// all content
		byte[] data = buff.toByteArray();
		int allsize = 4 + data.length;
		b = Numeric.toBytes(allsize);
		
		stream.write(b, 0, b.length);
		stream.write(data, 0, data.length);
		
		return allsize + 1;
	}

	/*
	 * resolve data stream
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

		// 值域(值长度和值)
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

		// 索引域(索引长度和索引值)
		if (seek + 4 > end) {
			throw new SizeOutOfBoundsException("index field sizeout!");
		}
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		// index field (index content)
		if(size > 0) {
			if (seek + size > end) {
				throw new SizeOutOfBoundsException("index field sizeout!");
			}
			this.setIndex(b, seek, size);
			seek += size;
		}

		// LIKE关键字域(LIKE关键字在索引基础上分割形成)
		if (seek + 4 > end) {
			throw new SizeOutOfBoundsException("vword sizeout!");
		}
		int count = Numeric.toInteger(b, seek, 4);
		seek += 4;
		for (int i = 0; i < count; i++) {
			byte type = Type.parseType(b[seek]);
			VWord vword = null;
			switch (type) {
			case Type.VCHAR:
				vword = new VChar(); break;
			case Type.VSCHAR:
				vword = new VSChar(); break;
			case Type.VWCHAR:
				vword = new VWChar(); break;
			default:
				throw new ColumnException("resolve error! invalid column type: %d", type);
			}

			size = vword.resolve(b, seek, end - seek);
			seek += size;
			array.add(vword);
		}

		return seek - off;
	}

}