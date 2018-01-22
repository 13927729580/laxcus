/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.statement.sort;

import java.io.*;
import java.util.*;

import com.lexst.sql.row.*;

/**
 * SQL "GROUP BY" 语句的分组存储器，同一KEY值的行记录保存在一起。<br>
 *
 */
public class GroupStage implements Serializable {

	private static final long serialVersionUID = 1931238199499901711L;
	
	private ArrayList<Row> array = new ArrayList<Row>(5);
	
	/**
	 * 
	 */
	public GroupStage() {
		super();
	}
	
	public GroupStage(Row row) {
		this();
		this.add(row);
	}
	
	public boolean add(Row row) {
		return array.add(row);
	}
	
	public List<Row> list() {
		return this.array;
	}
	
	public int size() {
		return this.array.size();
	}
	
	public boolean isEmpty() {
		return this.array.isEmpty();
	}

	/**
	 * 收缩到合适的空间
	 */
	public void trim() {
		this.array.trimToSize();
	}
}