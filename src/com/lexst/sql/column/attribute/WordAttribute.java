/**
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;
import java.util.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.util.*;

public abstract class WordAttribute extends VariableAttribute {
	
	private static final long serialVersionUID = -3041832490414058739L;

	/** 忽略大小写(CASE or NOT CASE)。默认是TRUE(忽略) **/
	protected boolean sentient;

	/** 模糊检索(LIKE or NOT LIKE)。默认是FALSE **/
	protected boolean like;
	
	/** vague word array **/
	protected List<VWord> vagues = new ArrayList<VWord>();

	/**
	 * @param type
	 */
	public WordAttribute(byte type) {
		super(type);
		this.sentient = true;
		this.like = false;
	}

	/**
	 * @param attribute
	 */
	public WordAttribute(WordAttribute attribute) {
		super(attribute);
		this.sentient = attribute.sentient;
		this.like = attribute.like;
		this.vagues.addAll(attribute.vagues);
	}

	/**
	 * @param type
	 * @param columnId
	 * @param name
	 */
	public WordAttribute(byte type, short columnId, String name) {
		this(type);
		super.setColumnId(columnId);
		super.setName(name);
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
	 * 模糊检索判断，默认不允许
	 * 
	 * @param b
	 */
	public void setLike(boolean b) {
		this.like = b;
	}

	/**
	 * assert like mode
	 * 
	 * @return
	 */
	public boolean isLike() {
		return this.like;
	}
	
	/**
	 * save vague word
	 * @param param
	 * @return
	 */
	public boolean addVWord(VWord param) {
		return vagues.add(param);
	}

	/**
	 * save vague word set
	 * @param set
	 * @return
	 */
	public boolean addVWords(Collection<VWord> set) {
		if (set == null || set.isEmpty()) {
			vagues.clear();
			return true;
		} else {
			return vagues.addAll(set);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.VariableAttribute#build()
	 */
	@Override
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(256);
		
		// 取出VariableAttribute参数
		super.build(buff);
		
		// 大小写敏感
		buff.write((byte) (this.sentient ? 1 : 0));
		// "SQL LIKE"支持
		buff.write((byte) (this.like ? 1 : 0));

		// "LIKE"关键字集合
		ByteArrayOutputStream body = new ByteArrayOutputStream(128);
		int elements = vagues.size();
		for (int i = 0; i < elements; i++) {
			VWord word = vagues.get(i);
			word.build(body);
		}
		byte[] b = Numeric.toBytes(elements);
		buff.write(b, 0, b.length);
		if (elements > 0) {
			b = body.toByteArray();
			buff.write(b, 0, b.length);
		}

		return buff.toByteArray();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.VariableAttribute#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		// 解析属于VariableAttribute的参数部分
		int size = super.resolve(b, seek, end - seek);
		seek += size;
		
		if(seek + 2 > end) {
			throw new ColumnAttributeResolveException("sentient or like sizeout!");
		}
		// 大小写敏感
		this.sentient = (b[seek] == 1);
		seek += 1;
		// 支持"SQL LIKE"
		this.like = (b[seek] == 1);
		seek += 1;

		// "LIKE"关键字
		int elements = Numeric.toInteger(b, seek, 4);
		seek += 4;
		short likeId = (short) (getColumnId() | 0x8000);
		for (int i = 0; i < elements; i++) {
			if (seek + 1 > end) {
				throw new ColumnAttributeResolveException("like word sizeout!");
			}
			byte type = Type.parseType(b[seek]);

			VWord column = (VWord) ColumnCreator.create(type);
			column.setId(likeId);

			size = column.resolve(b, seek, end - seek);
			seek += size;
			// 保存LIKE关键字
			this.vagues.add(column);
		}
		
		return seek - off;
	}

}