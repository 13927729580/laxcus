/**
 * 
 */
package com.lexst.sql.statement.sort;

import java.util.*;

import com.lexst.sql.column.Column;
import com.lexst.sql.row.Row;
import com.lexst.sql.statement.select.*;

/**
 * SQL "ORDER BY"实例排序器。<br>
 * 根据两个记录同一列，比较然后排列两行记录的先后顺序。<br>
 * <br>
 * 调用此类接口是: java.util.Arrays.sort(T[] a, java.util.Comparator<? supert T> c) <br>
 */
public class OrderBySorter implements Comparator<Row> {

	/** "ORDER BY" 实例 **/
	private OrderBy instance;
	
	/** 列标识->列自定义比较器 (区别与Column中的标准比较，这里用户自定义的比较，如字符和二进制的比较，用户需自定义类实现) **/
	private Map<java.lang.Short, ColumnComparator> set = new TreeMap<java.lang.Short, ColumnComparator>();
	
	/**
	 * defalut
	 */
	public OrderBySorter() {
		super();
	}

	/**
	 * @param order
	 */
	public OrderBySorter(OrderBy order) {
		this();
		this.setOrder(order);
	}
	
	/**
	 * 设置 "order by"实例
	 * @param order
	 */
	public void setOrder(OrderBy order) {
		this.instance = order;
	}

	/**
	 * 返回ORDER BY实例
	 * @return
	 */
	public OrderBy getOrder() {
		return this.instance;
	}

	/**
	 * add a self comparator
	 * 
	 * @param comparator
	 */
	public boolean add(ColumnComparator comparator) {
		short columnId = comparator.getColumnId();
		return set.put(columnId, comparator) == null;
	}

	/**
	 * remove a comparator
	 * 
	 * @param columnId
	 * @return
	 */
	public boolean remove(short columnId) {
		return set.remove(columnId) != null;
	}
	
	/**
	 * find a column comparator
	 * @param columnId
	 * @return
	 */
	public ColumnComparator get(short columnId){
		return set.get(columnId);
	}
	
	public java.util.Set<Short> keys() {
		return set.keySet();
	}

	public java.util.Collection<ColumnComparator> values() {
		return this.set.values();
	}

	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(Row row1, Row row2) {
		OrderBy next = this.instance;
		int ret = -1;
		while (next != null) {
			short columnId = next.getColumnId();
			Column c1 = row1.find(columnId);
			Column c2 = row2.find(columnId);
			
			// check compare
			if (set.isEmpty()) {
				ret = c1.compare(c2);
			} else { // find a comparator
				ColumnComparator comparator = set.get(columnId);
				if (comparator != null) {
					ret = comparator.compare(c1, c2); // user compare
				} else {
					ret = c1.compare(c2); // default compare
				}
			}
			
			// 如果当前列比较一致，取下一列比较
			if (ret == 0) {
				next = next.getNext();
			} else if (next.isASC()) { // 升序排列
				return ret;
			} else {
				return (ret > 0 ? -1 : 1);
			}
		}
		return 0;
	}

}