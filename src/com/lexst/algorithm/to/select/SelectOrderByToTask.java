/**
 * 
 */
package com.lexst.algorithm.to.select;


import com.lexst.algorithm.disk.*;
import com.lexst.algorithm.to.*;
import com.lexst.log.client.*;
import com.lexst.sql.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.conduct.matrix.*;
import com.lexst.sql.conduct.value.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.sort.*;
import com.lexst.sql.row.*;

import java.io.*;
import java.util.*;

/**
 * SQL "ORDER BY"排序比较器。计算每一行记录的位置，返回排序结果<br>
 */
public class SelectOrderByToTask extends ToTask {

	private Sheet sheet;
	
	private Select select;

	private List<Row> array = new ArrayList<Row>(1024);

	/**
	 * 
	 */
	public SelectOrderByToTask() {
		super();
	}

	/*
	 * 从DATA/WORK节点取回分组后的数据流，解析并且转为记录保存
	 * 
	 * @see com.lexst.algorithm.aggregate.ConductAggregateTask#inject(com.lexst.sql.distribute.matrix.Field, byte[], int, int)
	 */
	@Override
	public boolean inject(DiskField field, byte[] b, int off, int len) throws ToTaskException {
		int seek = off;
		int end = off + len;

		AnswerFlag flag = new AnswerFlag();
		int size = flag.resolve(b, seek, end - seek);
		seek += size;
		
		if (sheet == null) {
			// 取第一个SELECT(此时只能有一个SELECT)
			CValue value = super.getToPhase().find("SELECT_OBJECT");
			byte[] bs = ((CRaw)value).getValue();
			
			select = new Select();
			select.resolve(bs, 0, bs.length);
			
			// 查找数据表配置
			Space space = flag.getSpace();
			Table table = getFromChooser().findTable(space);
			if(table == null) {
				Logger.error("OrderAggregateTask.add, cannot find '%s'", space);
				return false;
			}
			
			sheet = new Sheet();
			for (short columnId : select.getShowId()) {
				ColumnAttribute attribute = table.find(columnId);
				if(attribute == null) {
					Logger.error("OrderAggregateTask.add, cannot find attribute:%d", columnId);
					return false;
				}
				sheet.add(sheet.size(), attribute);
			}
		}

		// 解析行记录
		RowParser parser = new RowParser(flag, sheet);
		size = parser.split(b, seek, end - seek);
		seek += size;
		// 输入数据并且保存
		List<Row> rows = parser.flush();
		this.array.addAll(rows);
		
		return true;
	}
	

	
	private OrderBySorter generate(Select select) {
		// 排序器
		OrderBySorter sorter = new OrderBySorter(select.getOrderBy());
		
		Space space = select.getSpace();
		Table table = super.getFromChooser().findTable(space);
		if(table == null) {
			Logger.error("cannot find %s", space);
			return null;
		}
		
		// 设置自定义的比较器
		for (short columnId : select.getOrderBy().listColumnIds()) {
			ColumnAttribute attribute = table.find(columnId);
			if(attribute == null) {
				Logger.error("cannot find attribute:%d", columnId);
				return null;
			}
			if (attribute.isChar()) {
				CharAttribute consts = (CharAttribute) attribute;
				CharComparator comparator = new CharComparator();
				comparator.setColumnId(columnId);
				comparator.setSentient(consts.isSentient());
				comparator.setPacking(consts.getPacking());
				sorter.add(comparator);
			} else if (attribute.isSChar()) {
				SCharAttribute consts = (SCharAttribute) attribute;
				SCharComparator comparator = new SCharComparator();
				comparator.setColumnId(columnId);
				comparator.setSentient(consts.isSentient());
				comparator.setPacking(consts.getPacking());
				sorter.add(comparator);
			} else if (attribute.isWChar()) {
				WCharAttribute consts = (WCharAttribute) attribute;
				WCharComparator comparator = new WCharComparator();
				comparator.setColumnId(columnId);
				comparator.setSentient(consts.isSentient());
				comparator.setPacking(consts.getPacking());
				sorter.add(comparator);
			}
		}
		
		return sorter;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.algorithm.to.ToTask#complete(com.lexst.algorithm.disk.DiskTrustor)
	 */
	@Override
	public byte[] complete( DiskTrustor trustor) throws ToTaskException {
		// 取出SELECT
//		Select select = conduct.getFrom().getSelect(0);
		
		// 产生一个排序器
		OrderBySorter sorter = this.generate(select);
		if(sorter == null) {
			Logger.error("generate OrderBySorter, failed!");
			return null;
		}
		// 执行排序操作
		java.util.Collections.sort(array, sorter);
		
		// 记录头
		AnswerFlag flag = new AnswerFlag();
		flag.setRows(array.size());
		flag.setColumns((short) array.get(0).size());
		flag.setStorage(Type.NSM);
		flag.setSpace(select.getSpace());
		byte[] b = flag.build();
		
		// 确定所需要的空间
		int total = b.length;
		for(Row row : array) {
			total += row.capacity();
		}
		total = total - total % 128 + 128;
		
		// 以"NSM"方式输出(行存储方式)
		ByteArrayOutputStream buff = new ByteArrayOutputStream(total);
		buff.write(b, 0, b.length);
		for (Row row : array) {
			row.build(buff);
		}

		// 行记录数据流
		byte[] data = buff.toByteArray();

		// 更新数据流头信息
		flag.setSize(data.length - b.length);
		b = flag.build();
		System.arraycopy(b, 0, data, 0, data.length - b.length);
		
		return data;
	}

}