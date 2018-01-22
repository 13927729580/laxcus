/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.charset.codepoint;

import java.util.*;

import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.util.*;

/**
 * @author scott.liang
 *
 */
public class InjectGenerator {
	
	/** 表名空间 **/
	private Space space;

	/** 列属性排列顺序表 **/
	private Sheet sheet;
	
	/**
	 * 
	 */
	public InjectGenerator() {
		// TODO Auto-generated constructor stub
	}
	
	public Space getSpace() {
		return this.space;
	}
	
	public Sheet getSheet() {
		return this.sheet;
	}
	
	public byte[] build() {
		return null;
	}
	
	/**
	 * 解析Inject的头标记，成功返回解析长度，失败返回-1
	 * 
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolveTag(byte[] data, int off, int len) {
		int seek = off;
		int end = off + len;
		
		if(seek + 10 > end) {
			return -1;
		}
		// all size
		int allsize = Numeric.toInteger(data, seek, 4);
		seek += 4;
		if(len != allsize) {
//			flushInsert(-1, resp);
			return -1; // 出错
		}
		// version
		int version = Numeric.toInteger(data, seek, 4);
		seek += 4;
		if (version != 1) {
//			flushInsert(-1, resp);
			return -1;
		}
		// 表空间名长度
		int schemaSize = data[seek++] & 0xFF;
		int tableSize = data[seek++] & 0xFF;
		
		if(seek + schemaSize + tableSize > end) {
			return -1;
		}
		
		String schema = new String(data, seek, schemaSize);
		seek += schemaSize;
		String table = new String(data, seek, tableSize);
		seek += tableSize;
		this.space = new Space(schema, table);

		return seek - off;
	}
	
	/**
	 * 解析列ID记录。成功返回解析字节长度，失败返回-1
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolveSheet(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		if(seek + 6 > end) return -1;
		short elements = Numeric.toShort(b, seek, 2);
		seek += 2;
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if(seek + size > end) return -1;
		
		this.sheet = new Sheet();
		
		short index = 0;
		for(int i = 0; i < elements; i++) {
			if(seek + 4 > end) return -1;
			
			// 列类型
			byte type = b[seek];
			seek += 1;
			// 类ID
			short columnId = Numeric.toShort(b, seek, 2);
			seek += 2;
			// 列名长度
			int columnSize = b[seek] & 0xFF;
			seek += 1;
			// 列名
			if(seek + columnSize > end) return -1;
			String name = new String(b, seek, columnSize);
			seek += columnSize;
			
			ColumnAttribute attribute = ColumnAttributeCreator.create(type);
			if(attribute == null) return -1;
			attribute.setColumnId(columnId);
			attribute.setName(name);
			sheet.add(index, attribute);
		}
		
		return seek  - off;
	}
	
	public int resolveHead(byte[] b, int off, int len) {
		int seek = off;
		int size = resolveTag(b, seek, len - (seek - off));
		seek += size;
		size = resolveSheet(b, seek, len - (seek - off));
		seek += size;
		return seek - off;
	}
	
	/**
	 * 从指定下标位置解析"行(ROW)"，并且保存
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @param array
	 * @return
	 */
	public int resolveRows(byte[] b, int off, int len, List<Row> array) {
		int seek = off;
		int end = off + len;
		
		if(seek + 8 > end) return -1;
		
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		int elements = Numeric.toInteger(b, seek, 4);
		seek += 4;
		
		if(seek + size > end) return -1;
		
		for(int i = 0; i < elements; i++) {
			Row row = new Row();
			try {
				size = row.resolve(this.sheet, b, seek, end - seek);
			} catch (RowParseException exp) {
				return -1;
			}
			seek += size;
			
			// 压缩空间，再保存
			row.trim();
			array.add(row);
		}
		
		return seek - off;
	}
	
	/**
	 * 解析列
	 * @param b
	 * @param off
	 * @param len
	 * @param saves
	 * @param array
	 * @return
	 */
	public int resolveRows(byte[] b, int off, int len, Collection<java.lang.Short> saves, List<Row> array) {
		int seek = off;
		int end = off + len;

		if (seek + 8 > end) return -1;

		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		int elements = Numeric.toInteger(b, seek, 4);
		seek += 4;

		if (seek + size > end) return -1;

		List<java.lang.Short> removes = new ArrayList<java.lang.Short>();
		for (int i = 0; i < elements; i++) {
			Row row = new Row();
			try {
				size = row.resolve(this.sheet, b, seek, end - seek);
			} catch (RowParseException exp) {
				return -1;
			}
			seek += size;
			// 保存需要保留的列
			for (Column column : row.list()) {
				if (!saves.contains(column.getId())) {
					removes.add(column.getId());
				}
			}
			// 删除需要清除的列
			for (java.lang.Short num : removes) {
				row.remove(num.shortValue());
			}
			row.trim();
			array.add(row);
		}

		return seek - off;
	}

	/**
	 * 解析全部数据
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @param rows
	 * @return
	 */
	public int resolve(byte[] b, int off, int len, List<Row> rows) {
		int seek = off;
		int end = off + len;
		int size = resolveHead(b, seek, end - seek);
		if (size == -1) return -1;
		seek += size;

		// 解析为行记录
		size = this.resolveRows(b, seek, end - seek, rows);
		if (size == -1) return -1;
		seek += size;

		return seek - off;
	}
}
