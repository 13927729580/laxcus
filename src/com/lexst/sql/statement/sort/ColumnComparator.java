/**
 * 
 */
package com.lexst.sql.statement.sort;

import com.lexst.sql.column.Column;

/**
 * 用户自定义的"列"排序比较器
 *
 */
public interface ColumnComparator {

	/**
	 * column identity
	 * @return
	 */
	short getColumnId();
	
	/**
	 * value compare
	 * @param o1
	 * @param o2
	 * @return
	 */
	int compare(Column o1, Column o2);
}