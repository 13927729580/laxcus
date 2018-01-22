/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.sort;

import java.util.*;

import com.lexst.sql.column.*;
import com.lexst.sql.row.*;

/**
 * @author scott.liang
 * 
 */
public class GroupKey {// implements java.lang.Comparable<GroupKey> {

	private Column[] columns;

	private int hash;
	
	

	/**
	 * @param columns
	 */
	public GroupKey(Column[] columns) {
		super();
		this.setColumns(columns);
	}
	
	/**
	 * @param columnIds
	 * @param row
	 */
	public GroupKey(short[] columnIds, Row row) {
		super();
		this.setColumns(columnIds, row);
	}
	
	public void setColumns(short[] columnIds, Row row) {
		List<Column> array = new ArrayList<Column>();
		for(short columnId: columnIds) {
			Column column = row.find(columnId);
			if(column == null) {
				throw new ColumnNotFoundException("cannot find: %d", columnId);
			}
			array.add(column);
		}
		this.columns = new Column[array.size()];
		array.toArray(this.columns);
	}

	public void setColumns(Column[] elements) {
		if (elements == null) {
			this.columns = null;
		} else {
			this.columns = new Column[elements.length];
			System.arraycopy(elements, 0, this.columns, 0, this.columns.length);
		}
	}

	public Column[] getColumns() {
		return this.columns;
	}

	public boolean equals(GroupKey key) {
		if (columns.length != key.columns.length) {
			return false;
		}
		for (int i = 0; i < columns.length; i++) {
			if (!columns[i].equals(key.columns[i])) return false;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		return this.equals((GroupKey) object);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (this.hash == 0 && columns != null) {
			if (columns.length > 0) {
				this.hash = columns[0].hashCode();
			}
			for (int i = 1; i < columns.length; i++) {
				this.hash ^= columns[i].hashCode();
			}
		}
		return this.hash;
	}

//	/* (non-Javadoc)
//	 * @see java.lang.Comparable#compareTo(java.lang.Object)
//	 */
//	@Override
//	public int compareTo(GroupKey key) {
//		if (columns.length < key.columns.length) return -1;
//		else if (columns.length > key.columns.length) return 1;
//
//		// 在这里调用ColumnEqualtor(里面处理大小写敏感，解包等)，然后逐一比较各列
//		GroupKeyComparator equaltor = new GroupKeyComparator(null);
//		
//		return equaltor.comparate(columns, key.columns);
//		
////		for (int i = 0; i < columns.length; i++) {
////			int ret = columns[i].compareTo(key.columns[i]);
////			if (ret != 0) return ret;
////		}
////		return 0;
//		
//	}

}