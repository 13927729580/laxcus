/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.sort;

import java.util.*;

import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.*;
import com.lexst.sql.function.value.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.select.*;

/**
 * SQL "GROUP BY" 归类排列器
 *
 */
public class GroupSorter {

//	/** GROUP BY句柄和它的列ID号 */
//	private short[] columnIds;
//	private GroupBy handle;
	
	private Select select;
	
	/** 数据库表句柄 */
	private Table table;

	/** 记录分类存储器 */
	private Map<GroupKey, GroupStage> elements;
	
//	/**
//	 * 构造SQL "GROUP BY" 归类排列器，包括GROUP BY句柄和数据表句柄<br>
//	 * @param groupby
//	 * @param table
//	 */
//	public GroupSorter(GroupBy groupby, Table table) {
//		super();
//		this.setGroupBy(groupby);
//		this.setTable(table);
//	}
	
	/**
	 * 构造SQL "GROUP BY" 归类排列器，包括GROUP BY句柄和数据表句柄<br>
	 * 
	 * @param select
	 * @param table
	 */
	public GroupSorter(Select select, Table table) {
		super();
		this.setSelect(select);
		this.setTable(table);
	}
	
	public void setSelect(Select s) {
		this.select = s;
	}
	public Select getSelect() {
		return this.select;
	}
	
//	/**
//	 * 设置GROUP BY句柄
//	 * @param i
//	 */
//	public void setGroupBy(GroupBy i) {
//		this.handle = i;
//		columnIds = this.handle.listGroupIds();
//	}
	
	/**
	 * 设置TABLE句柄
	 * @param t
	 */
	public void setTable(Table t) {
		this.table = t;
	}
	
	/**
	 * 根据行中的列，产生一个GroupKey
	 * @param row
	 * @return
	 */
	private GroupKey createKey(Row row) {
		short[] columnIds = select.getGroupBy().listGroupIds();
		Column[] keys = new Column[columnIds.length];
		for(int i = 0; i < columnIds.length; i++) {
			keys[i] = row.find(columnIds[i]);
		}
		return new GroupKey(keys);
	}
	
	/**
	 * 数据归类分组接口
	 * @param array
	 * @param flush
	 */
	public List<Row> align(List<Row> array) {
		// 初始化存储记录集(定义排序比较器)
		if (elements == null) {
			GroupKeyComparator comparator = new GroupKeyComparator(table);
			elements = new TreeMap<GroupKey, GroupStage>(comparator);
		}
		
		// 按照KEY进行记录分组
		for(Row row : array) {
			// 取出属于KEY的列
			GroupKey key = createKey(row);
			GroupStage stage = elements.get(key);
			if(stage == null) {
				stage = new GroupStage();
				elements.put(key, stage);
			}
			stage.add(row);
		}
		
		// 收缩空间，节省内存
		for (GroupStage stage : elements.values()) {
			stage.trim();
		}
		
		GroupBy handle = select.getGroupBy();
		
		// 根据HAVING子句，进一步筛选合格的结果
		ArrayList<GroupKey> removes = new ArrayList<GroupKey>();
		for (GroupKey key : elements.keySet()) {
			GroupStage stage = elements.get(key);
			
			Situation situation = handle.getSituation();
			boolean ret = flite(situation, stage.list());

			while (situation != null) {
				for (Situation sub : situation.getPartners()) {
					boolean rs = flite(sub, stage.list());
					if (sub.isAND()) ret = (ret && rs);
					else if (sub.isOR()) ret = (ret || rs);
				}
				situation = situation.getNext();
			}

			if (!ret) removes.add(key);
		}
		
		// 删除不匹配的结果，保留匹配的
		for (GroupKey key : removes) {
			elements.remove(key);
		}
		
		// 更新集合并且输出
		List<Row> flush = new ArrayList<Row>(elements.size());
		
		// 剩下的记录集，合并为一行记录
		for(GroupStage stage: elements.values()) {
			Row row = new Row();
			List<Row> rows = stage.list();
			SQLRowSet set = new SQLRowSet(rows);
			
			ShowSheet sheet = select.getShowSheet();
			for(ShowElement element : sheet.list()) {
				if(element.isColumn()) {
					Column column = rows.get(0).find(element.getColumnId());
					row.add(column);
				} else if(element.isFunction()) {
					SQLValue value = ((FunctionElement) element).getFunction().compute(set);
					short columnId = element.getColumnId();
					if(columnId == 0) element.getIdentity();
					if (value.isString()) {
						ColumnAttribute attribute = table.find(columnId);
						if (attribute != null && attribute.isVariable()) {
							Packing packing = ((VariableAttribute) attribute).getPacking();
							Column column = ((SQLString) value).toColumn(columnId, packing);
							row.add(column);
						} else {
							row.add(value.toColumn(columnId));
						}
					} else if(value.isRaw()) {
						ColumnAttribute attribute = table.find(columnId);
						if (attribute != null && attribute.isVariable()) {
							Packing packing = ((VariableAttribute) attribute).getPacking();
							Column column = ((SQLRaw) value).toColumn(columnId, packing);
							row.add(column);
						} else {
							row.add(value.toColumn(columnId));
						}
					} else {
						Column column = value.toColumn(columnId);
						row.add(column);
					}
				}
			}
			// 保存一行记录
			flush.add(row);
		}
		
		return flush;
	}

	private boolean flite(Situation situ, List<Row> rows) {
		return situ.sifting(rows);
	}
	
}