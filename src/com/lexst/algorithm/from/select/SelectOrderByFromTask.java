/**
 * 
 */
package com.lexst.algorithm.from.select;

import com.lexst.algorithm.disk.*;
import com.lexst.algorithm.from.*;
import com.lexst.algorithm.util.*;
import com.lexst.log.client.*;
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
import java.util.*;

/**
 * 处理"ORDER BY"数据分片，数据保存到磁盘，返回Area结果
 *
 */
public abstract class SelectOrderByFromTask extends FromTask {

//	/** 全部分片类 **/
//	private static Class<?>[] sections = new Class<?>[] { CharSector.class,
//		SCharSector.class, WCharSector.class, ShortSector.class,
//		IntegerSector.class, LongSector.class, FloatSector.class,
//		DoubleSector.class, DateSector.class, TimeSector.class,
//		TimestampSector.class };

	/**
	 * default
	 */
	public SelectOrderByFromTask() {
		super();
	}

//	/**
//	 * @param project
//	 */
//	public SelectOrderByFromTask(Project project) {
//		super(project);
//	}

	/*
	 * 解析ROW信息，生成数据分片并且返回，实际数据保存在本地磁盘上，等待WORK节点来读取
	 * 
	 * @see com.lexst.algorithm.diffuse.ConductDiffusetTask#divideup(com.lexst.util.host.SiteHost, com.lexst.algorithm.disk.DiskTrustor, com.lexst.sql.statement.Conduct, byte[], int, int)
	 */
	@Override
	public DiskArea divideup( DiskTrustor trustor, FromPhase phase, byte[] b, int off, int len) throws FromTaskException {
		int seek = off;
		int end = off + len;
		
		// 解析数据头
		AnswerFlag flag = new AnswerFlag();
		int size = flag.resolve(b, seek, end - seek);
		seek += size;
		
		// 查找数据表配置
		Space space = flag.getSpace();
		Table table = super.getParent().findTable(space);
		if(table == null) {
			Logger.error("OrderDiffuseTask.execute, cannot find %s", space);
			return null;
		}
		if (table.getStorage() != flag.getStorage()) {
			throw new IllegalArgumentException("invalid storage model");
		}
		
		//1. 生成Sheet
		Sheet sheet = new Sheet();
		Select select = phase.getSelect();
		for(short columnId : select.getShowId()) {
			ColumnAttribute attribute = table.find(columnId);
			if(attribute == null) {
				Logger.error("OrderDiffuseTask.execute, cannot find %d", columnId);
				return null;
			}
			sheet.add(sheet.size(), attribute);
		}
		
		//2. 解析数据，判断存储模型(存储模型必须一致)
		RowParser parser = new RowParser(flag, sheet, false);
		size = parser.split(b, seek, end - seek);
		seek += size;
		List<Row> rows = parser.flush();

		//3. 根据FROM中的命名，找到分片集(初始化在Init命名)
		ColumnSector sector = phase.getSlaveSector(); 
		// 记录缓存
		Map<java.lang.Integer, RowBuffer> collects = new TreeMap<java.lang.Integer, RowBuffer>();
		
		//4. 从行中取出列，根据列值选择对应的分片下标，将记录存入对应的分片
		OrderBy order = select.getOrderBy();
		short columnId = order.getColumnId();
		for(Row row : rows) {
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
		
		//5. 数据写入磁盘，返回任务号
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
		
		return area;
	}

}