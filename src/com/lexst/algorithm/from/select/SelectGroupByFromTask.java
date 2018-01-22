/**
 * @email admin@wigres.com
 *
 */
package com.lexst.algorithm.from.select;

import java.util.*;

import com.lexst.algorithm.disk.*;
import com.lexst.algorithm.from.*;
import com.lexst.algorithm.util.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;
import com.lexst.sql.index.section.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.select.*;
import com.lexst.util.host.*;

/**
 * SQL "GROUP BY"操作
 * 
 * diffuse/aggregate算法规定，针对conduct，diffuse只有一次执行，aggregate允许多次
 */
public class SelectGroupByFromTask extends FromTask {

	/**
	 * 
	 */
	public SelectGroupByFromTask() {
		super();
	}

	/*
	 * 根据GROUP BY的第一个参数进行数据分片，返回分片范围集合
	 * 
	 * @see com.lexst.algorithm.diffuse.ConductDiffusetTask#divideup(com.lexst.util.host.SiteHost, com.lexst.algorithm.disk.DiskTrustor, com.lexst.sql.statement.Conduct, byte[], int, int)
	 */
	@Override
	public DiskArea divideup(DiskTrustor trustor, FromPhase phase, byte[] b, int off, int len) throws FromTaskException {
		int seek = off;
		int end = off + len;
		
		// 解析数据头标记
		AnswerFlag flag = new AnswerFlag();
		int size = flag.resolve(b, off, len);
		seek += size;
		
		// 找到表配置
		Space space = flag.getSpace();
		Table table = super.getParent().findTable(space);
		if(table == null) {
			// 出错
		}
		// 存储类型必须一致
		if(table.getStorage() != flag.getStorage()) {
			// 出错
		}
		
		// 根据显示列的排列，生成SHEET. SELECT在FROM中只能有一个
		Sheet sheet = new Sheet();

		Select select = phase.getSelect();
		for (short columnId : select.getShowId()) {
			ColumnAttribute attribute = table.find(columnId);
			if (attribute == null) {
				// 出错
			}
			// 从0下标开始，依次存储
			sheet.add(sheet.size(), attribute);
		}
		
		// 解析数据流
		RowParser parser = new RowParser(flag, sheet);
		size = parser.split(b, seek, end - seek);
		seek += size;
		List<Row> array = parser.flush();
		
		// 以第一个分组进行分片
		ColumnSector sector = phase.getSlaveSector();
		Map<java.lang.Integer, RowBuffer> collects = new TreeMap<java.lang.Integer, RowBuffer>();

		GroupBy groupby = select.getGroupBy();
		short columnId = groupby.listGroupIds()[0];
		for(Row row : array) {
			Column column = row.find(columnId);
			// 找到分片所在下标位置
			int index = sector.indexOf(column);

			RowBuffer buff = collects.get(index);
			if(buff == null) {
				buff = new RowBuffer(index, space);
				collects.put(index, buff);
			}
			buff.add(row);
		}

		//5. 申请任务号，数据写入磁盘，返回任务号
		SiteHost local = trustor.getLocal();
		long jobid = trustor.nextJobid();
		
		DiskArea area = new DiskArea(jobid, local, trustor.timeout());

		for(int index : collects.keySet()) {
			RowBuffer buff = collects.get(index);
			// 产生数据流，写入磁盘文件，返回数据在文件的下标位置(开始和结束位置)
			byte[] data = buff.build();
			long[] position = trustor.write(jobid, index, data, 0, data.length);
			// 记录一段数据区域
			area.add(new DiskField(index, position[0], position[1]));
		}
		
		return area;
	}

}
