/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.statement.sort;

import java.io.*;
import java.util.*;

import com.lexst.log.client.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.util.*;

/**
 * SQL "GROUP BY" 的键值集合比较器
 *
 */
public class GroupKeyComparator implements Comparator<GroupKey> {

	private Table table;
	
	/**
	 * 
	 */
	public GroupKeyComparator(Table table) {
		super();
		this.setTable(table);
	}

	public void setTable(Table t) {
		this.table = t;
	}
	
	public Table getTable() {
		return this.table;
	}
	
	/**
	 * 比较字符列是否一致
	 * 
	 * @param c1
	 * @param c2
	 * @param attribute
	 * @return
	 */
	private int compareWord(Column c1, Column c2, ColumnAttribute attribute) {
		byte[] b1 = ((Word)c1).getValue();
		byte[] b2 = ((Word)c2).getValue();
		
		WordAttribute attri = (WordAttribute)attribute;
		
		// 解包(解压和解密操作)
		Packing packing = attri.getPacking();
		if(packing.isEnabled()) {
			try {
				b1 = VariableGenerator.depacking(packing, b1, 0, b1.length);
				b2 = VariableGenerator.depacking(packing, b2, 0, b2.length);
			} catch (IOException e) {
				Logger.error(e);
				return -1;
			}
		}
		// 解码
		Charset charset = null;
		if(attri.isChar()) charset = new UTF8();
		else if(attri.isSChar()) charset = new UTF16();
		else if(attri.isWChar()) charset = new UTF32();
		String f1 = charset.decode(b1, 0, b1.length);
		String f2 = charset.decode(b2, 0, b2.length);
		// 比较参数是否匹配，大小写敏感
		return (attri.isSentient() ? f1.compareTo(f2) : f2.compareToIgnoreCase(f2));
	}

	/* 比较两组GroupKey
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(GroupKey key1, GroupKey key2) {
		Column[] s1 = key1.getColumns();
		Column[] s2 = key2.getColumns();

		if (s1.length < s2.length) return -1;
		else if (s1.length > s2.length) return 1;

		for (int i = 0; i < s1.length; i++) {
			for (int j = 0; j < s2.length; j++) {
				if (s1[i].getId() != s2[j].getId()) continue;

				// 检查属性
				int ret = 0;
				ColumnAttribute attribute = table.find(s1[i].getId());
				if (attribute.isNumber() || attribute.isRaw()) {
					ret = s1[i].compare(s2[j]); // 继续排列位置
				} else if (attribute.isWord()) {
					ret = this.compareWord(s1[i], s2[j], attribute);
				}
				// 一致，继续比较。否则返回
				if (ret == 0) break;
				else return ret;
			}
		}

		return 0;
	}

}