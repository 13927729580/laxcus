/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.select;

import java.io.*;
import java.util.*;

import com.lexst.sql.schema.*;
import com.lexst.sql.column.attribute.*;

public class ShowSheet implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	/** 显示成员集合 (包含列成员和函数成员) **/
	private List<ShowElement> array = new ArrayList<ShowElement>();

	/**
	 * default
	 */
	public ShowSheet() {
		super();
	}

	/**
	 * 
	 * @param sheet
	 */
	public ShowSheet(ShowSheet sheet) {
		this();
		this.array.addAll(sheet.array);
	}

	/**
	 * generate a "Sheet" by elements
	 *  
	 * @return Sheet
	 */
	public Sheet getSheet() {
		int index = 0;
		Sheet sheet = new Sheet();
		for (ShowElement element : array) {
			ColumnAttribute attribute = ColumnAttributeCreator.create(element.getType());
			if (attribute == null) {
				throw new ColumnAttributeException("unknown column attribute:%d", element.getType());
			}
			attribute.setColumnId(element.getIdentity());
			attribute.setName(element.getName());
			sheet.add(index++, attribute);
		}

		return sheet;
	}

	/**
	 * 返回列成员的ID数组
	 * 
	 * @return short[]
	 */
	public short[] getShowIds() {
		List<java.lang.Short> a = new ArrayList<java.lang.Short>();
		for (ShowElement element : this.array) {
			short columnId = element.getColumnId();
			if(columnId != 0 && !a.contains(columnId)) {
				a.add(columnId);
			}
		}

		int size = a.size();
		if (size == 0) return null;

		short[] s = new short[size];
		for (int i = 0; i < s.length; i++) {
			s[i] = a.get(i).shortValue();
		}
		return s;
	}

	/**
	 * 检查某个成员ID是否存在，包括函数成员和列成员
	 * 
	 * @param elementId
	 * @return
	 */
	public boolean exists(short elementId) {
		for (ShowElement element : this.array) {
			if (element.getIdentity() == elementId) return true;
		}
		return false;
	}

	public boolean add(ShowElement element) {
		return this.array.add(element);
	}

	public List<ShowElement> list() {
		return this.array;
	}
	
	public ShowElement get(int index) {
		if(index < 0 || index >= array.size()) {
			return null;
		}
		return array.get(index);
	}

	public boolean isEmpty() {
		return this.array.isEmpty();
	}

	public int size() {
		return this.array.size();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new ShowSheet(this);
	}
}