/**
 * @email admin@wigres.com
 *
 */
package com.lexst.algorithm.to.select;

import java.io.*;
import java.util.*;

import com.lexst.algorithm.disk.*;
import com.lexst.algorithm.to.*;
import com.lexst.algorithm.util.*;
import com.lexst.log.client.*;
import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;
import com.lexst.sql.conduct.value.*;
import com.lexst.sql.index.section.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.select.*;
import com.lexst.sql.statement.sort.*;
import com.lexst.util.host.*;

/**
 * @author scott.liang
 *
 */
public class SelectGroupByToTask extends ToTask {

	private Sheet sheet;
	
	private Select select;
	
	private List<Row> array = new ArrayList<Row>(1024);
	
	/**
	 * 
	 */
	public SelectGroupByToTask() {
		super();
	}


	/*
	 * 接收经过diffuse过程分片数据
	 * @see com.lexst.algorithm.aggregate.ConductAggregateTask#inject(com.lexst.sql.distribute.matrix.Field, byte[], int, int)
	 */
	@Override
	public boolean inject(DiskField field, byte[] b, int off, int len) throws ToTaskException {
		int seek = off;
		int end = off + len;
		
		AnswerFlag flag = new AnswerFlag();
		int size = flag.resolve(b, seek, end - seek);
		seek += size;
	
		if(sheet == null) {
			Space space = flag.getSpace();
			Table table = super.getFromChooser().findTable(space);
			if(table == null) {
				
			}
			
			// 取第一个SELECT(此时只能有一个SELECT)
			CValue value = super.getToPhase().find("SELECT_OBJECT");
			byte[] bs = ((CRaw)value).getValue();
			
			select = new Select();
			select.resolve(bs, 0, bs.length);
			
//			Select select = conduct.getFrom().getSelect(0);
			
			sheet = new Sheet();
			for (short columnId : select.getShowId()) {
				ColumnAttribute attribute = table.find(columnId);
				if (attribute == null) {

				}
				sheet.add(sheet.size(), attribute);
			}
		}
		
		// 解析数据流
		RowParser parser = new RowParser(flag, sheet, false);
		size = parser.split(b, seek, end - seek);
		seek += size;
		// 输出并且保存记录
		java.util.List<Row> rows = parser.flush();
		array.addAll(rows);
		
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.algorithm.to.ToTask#complete(com.lexst.algorithm.disk.DiskTrustor)
	 */
	@Override
	public byte[] complete(DiskTrustor trustor) throws ToTaskException {
		// 从配置池中取出数据库表
		Space space = select.getSpace();
		Table table = super.getFromChooser().findTable(space);
		if (table == null) {
			Logger.error("GroupByAggregateTask.complete, cannot find %s", space);
			return null;
		}

		// 数据重组，返回新的集合
		GroupSorter sorter = new GroupSorter(select, table);
		List<Row> flush = sorter.align(array);

		ToPhase phase = getToPhase();
		
		// 检查是否有子集迭代
		if(phase.hasNext()) {	
			// 取它下级的分割器
			ColumnSector sector = phase.getSlaveSector();
			// 记录缓存
			Map<java.lang.Integer, RowBuffer> collects = new TreeMap<java.lang.Integer, RowBuffer>();
			//4. 从行中取出列，根据列值选择对应的分片下标，将记录存入对应的分片
			OrderBy order = select.getOrderBy();
			short columnId = order.getColumnId();
			for(Row row : flush) {
				// 根据分片信息，判断每个列的存储下标位置
				Column column = row.find(columnId);
				int index = sector.indexOf(column);

				RowBuffer buff = collects.get(index);
				if(buff == null) {
					buff = new RowBuffer(index, space);
					collects.put(index, buff);
				}
				buff.add(row);
			}
			
			// 这个时候,这是ORDER BY检查，形成分片，返回分片的数据流
			long jobid = trustor.nextJobid();
			SiteHost local = trustor.getLocal();
			DiskArea area = new DiskArea(jobid, local, trustor.timeout());
			for(int index : collects.keySet()) {
				RowBuffer buff = collects.get(index);
				// 产生数据流，写入磁盘
				byte[] data = buff.build();
				long[] position = trustor.write(jobid, index, data, 0, data.length);
				// 保存分片信息
				area.add(new DiskField(index, position[0], position[1]));
			}
			return area.build();
		} else {
			// 整理并且新的数据流
			AnswerFlag flag = new AnswerFlag();
			flag.setRows(flush.size());
			flag.setColumns((short) flush.get(0).size());
			flag.setSpace(space);
			flag.setStorage(Type.NSM);

			// 统计总的数据流长度
			byte[] b = flag.build();
			int total = b.length;
			for (Row row : flush) {
				total += row.capacity();
			}
			total = total - total % 128 + 128;

			// 数据输出到缓存
			ByteArrayOutputStream buff = new ByteArrayOutputStream(total);
			buff.write(b, 0, b.length);
			for (Row row : array) {
				row.build(buff);
			}

			// 行记录数据流
			byte[] data = buff.toByteArray();
			// 更新尺寸
			flag.setSize(data.length - b.length);
			b = flag.build();
			System.arraycopy(b, 0, data, 0, data.length - b.length);

			return data;
		}
	}

}