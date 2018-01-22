/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.row;

import java.util.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.schema.*;
import com.lexst.util.*;

/**
 * 解析行/列存储流中的数据域内容。<br>
 * 在解析前，必须提供数据流报头:Flag和表的排列布局:Sheet。<br>
 *
 */
public class RowParser {

	private AnswerFlag flag;
	private Sheet sheet;
	
	/** 解析数据时，是否进行CRC32校验(校验会比较耗时)，默认是FALSE */
	private boolean checksum;

	/** 行记录的存储器 */
	private ArrayList<Row> collects = new ArrayList<Row>(1024);

	/**
	 * 
	 */
	protected RowParser() {
		super();
		this.checksum = false;
	}

	/**
	 * @param flag
	 * @param sheet
	 */
	public RowParser(AnswerFlag flag, Sheet sheet) {
		this();
		this.setFlag(flag);
		this.setSheet(sheet);
	}
	
	/**
	 * @param flag
	 * @param sheet
	 * @param checksum
	 */
	public RowParser(AnswerFlag flag, Sheet sheet, boolean checksum) {
		this(flag, sheet);
		this.setChecksum(checksum);
	}
	
	/**
	 * 设置数据的头信息
	 * @param s
	 */
	public void setFlag(AnswerFlag s) {
		this.flag = s;
	}
	
	/**
	 * 设置数据的排列表
	 * @param s
	 */
	public void setSheet(Sheet s) {
		this.sheet = s;
	}
	
	/**
	 * set CRC32 checksum, when DSM status
	 * @param b
	 */
	public void setChecksum(boolean b) {
		this.checksum = b;
	}

	/**
	 * get CRC32 checksum, only DSM status
	 * @return
	 */
	public boolean isChecksum() {
		return this.checksum;
	}

	/**
	 * 解析行/列存储的数据流，转换成类保存，返回解析的长度
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int split(byte[] b, int off, int len) {
		if (flag.isNSM()) {
			return splitNSM(b, off, len);
		} else {
			return splitDSM(b, off, len);
		}
	}
	
	/**
	 * 解析
	 * @param b
	 * @return
	 */
	public int split(byte[] b) {
		return split(b, 0, b.length);
	}
	
	/**
	 * 输入内存中的行记录，同时清除内存已有数据
	 * @return
	 */
	public List<Row> flush() {
		int size = collects.size();
		List<Row> array = new ArrayList<Row>(size);

		array.addAll(collects);
		collects.clear();
		return array;
	}
	
	/**
	 * 解析行存储模式数据，记录保存到内存中。返回数据流解析长度
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	private int splitNSM(byte[] b, int off, int len) {
		int seek = off;
		int end = off  + len;
		while(seek < end) {
			Row row = new Row();
			int size = row.resolve(sheet, b, seek, end - seek);
			if (size == -1) {
				throw new RowParseException("row resolve error! at:%d", seek);
			}
			seek += size;
			collects.add(row);
		}

		return seek - off;
	}

	/**
	 * 解析列存储模式数据，记录保存到内存，返回数据流解析长度
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	private int splitDSM(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		while (seek < end) {
			ChunkFlag chunk = new ChunkFlag();
			int size = chunk.resolve(b, seek, end - seek);
			if (size == -1) break; // 尺寸不足，退出! 
			seek += size;
			
//			System.out.printf("chunk size:%d, row count:%d, column count:%d\n", 
//					chunk.chunk_size, chunk.row_count, chunk.column_count);
			
			if (sheet.size() != chunk.column_count) {
				throw new RowParseException("column size not match! %d, %d", chunk.column_count, sheet.size());
			}
			
			// alloc row array
			Row[] array = new Row[chunk.row_count];
			for(int i = 0; i < chunk.row_count; i++) {
				array[i] = new Row();
			}
			
			// resolve column's block
			for(short index = 0; index < chunk.column_count; index++) {				
				ColumnFlag element = new ColumnFlag();
				int lens = element.resolve(b, seek, end - seek);
				seek += lens;
				
				// column field length
				int fieldlen = element.blockSize - element.presize();

				// check parameter
				if (chunk.row_count != element.item_count) {
					throw new RowParseException("not match column size!");
				}
				ColumnAttribute attribute = sheet.get(index);
				if (attribute.getColumnId() != element.columnId) {
					throw new RowParseException("not match column identity!");
				}
				
				Column[] columns = null;
				if (attribute.isVariable()) {
					columns = splitVariable(attribute, b, seek, fieldlen);
				} else if (attribute.isCalendar() || attribute.isNumber()) {
					columns = splitNum(attribute, b, seek, fieldlen);
				} else {
					throw new RowParseException("invalid column type!");
				}
				
				if (columns == null || columns.length != element.item_count) {
					throw new RowParseException("invalid column count!");
				}
				
				// 保存一组列数据到各行中
				for(int i = 0; i < array.length; i++) {
					array[i].add(columns[i]);
				}
				
				// 下一块列数据开始位置
				seek += fieldlen;
			}
			// 保存行记录
			for(int i = 0; i < array.length; i++) {
				collects.add(array[i]);
			}
		}
		
		return seek - off;
	}
	
	/**
	 * @param attribute
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	private Column[] splitVariable(ColumnAttribute attribute, byte[] b, int off, int len) {		
		List<Column> array = new ArrayList<Column>();
		short columnId = attribute.getColumnId();
		int seek = off;
		int end = off + len;
		
		VariableFlag variable = new VariableFlag();

		while (seek < end) {
			int size = variable.resolve(b, seek, end - seek);
			// check block size
			if (seek + variable.wordlen > end) {
				throw new RowParseException("block sizeout!");
			}
			seek += size;
			
			boolean nullable = Type.isNullable(variable.state);
			byte type = Type.parseType(variable.state);
			
			if(type != attribute.getType()) {
				throw new RowParseException("not match column property!");
			}
			
			byte[] data = null;
			if (!nullable && variable.wordlen > 0) {
				data = new byte[variable.wordlen];
				System.arraycopy(b, seek, data, 0, variable.wordlen);
				seek += variable.wordlen; // next block's offset
				
				if(this.checksum) {
					java.util.zip.CRC32 c32 = new java.util.zip.CRC32();
					c32.update(data);
					if(variable.crc32 != c32.getValue()) {
						throw new RowParseException("CRC32 error!");
					}
				}
			}
			
			// save a column
			for (int i = 0; i < variable.itemCount; i++) {
				switch (type) {
				case Type.RAW:
					array.add(new Raw(columnId, data)); break;
				case Type.CHAR:
					array.add(new Char(columnId, data)); break;
				case Type.SCHAR:
					array.add(new SChar(columnId, data)); break;
				case Type.WCHAR:
					array.add(new WChar(columnId, data)); break;
				}
			}
		}
		
		Column[] elements = new Column[array.size()];
		return array.toArray(elements);
	}

	/**
	 * resolve const value
	 * @param attribute
	 * @param count
	 * @param block
	 * @param off
	 * @param len
	 * @return
	 */
	private Column[] splitNum(ColumnAttribute attribute, byte[] block, int off, int len) {
		List<Column> array = new ArrayList<Column>();
		short columnId = attribute.getColumnId();
		int seek = off;
		int end = off + len;
		
		while (seek < end) {
			// check block head's size
			if (seek + 5 > end) {
				throw new RowParseException("block sizeout!");
			}
			
			// numeric struct
			int itemCount = Numeric.toInteger(block, seek, 4); // column count
			seek += 4;
			byte state = block[seek];
			seek += 1;
			
			boolean nullable = Type.isNullable(state);
			byte type = Type.parseType(state);
			
			if(type != attribute.getType()) {
				throw new RowParseException("not match column type!");
			}
			
			// check buffer size
			switch (type) {
			case Type.SHORT:
				if (seek + 2 > end) {
					throw new RowParseException("block sizeout!");
				}
				break;
			case Type.INTEGER:
			case Type.FLOAT:
			case Type.DATE:
			case Type.TIME:
				if (seek + 4 > end) {
					throw new RowParseException("block sizeout!");
				}
				break;
			case Type.LONG:
			case Type.DOUBLE:
			case Type.TIMESTAMP:
				if (seek + 8 > end) {
					throw new RowParseException("block sizeout!");
				}
				break;
			}
			
			switch(type) {
			case Type.SHORT:
				short v_short = Numeric.toShort(block, seek, 2);
				seek += 2;
				for (int i = 0; i < itemCount; i++) {
					com.lexst.sql.column.Short col = new com.lexst.sql.column.Short(columnId);
					if (!nullable) col.setValue( v_short );
					array.add(col);
				}
				break;
			case Type.INTEGER:
				int v_int = Numeric.toInteger(block, seek, 4);
				seek += 4;
				for (int i = 0; i < itemCount; i++) {
					com.lexst.sql.column.Integer col = new com.lexst.sql.column.Integer(columnId);
					if (!nullable) col.setValue(v_int);
					array.add(col);
				}
				break;
			case Type.LONG:
				long v_long = Numeric.toLong(block, seek, 8);
				seek += 8;
				for (int i = 0; i < itemCount; i++) {
					com.lexst.sql.column.Long col = new com.lexst.sql.column.Long(columnId);
					if (!nullable) col.setValue(v_long);
					array.add(col);
				}
				break;
			case Type.FLOAT:
				int n_real = Numeric.toInteger(block, seek, 4);
				seek += 4;
				float float_num = java.lang.Float.intBitsToFloat(n_real);
				for (int i = 0; i < itemCount; i++) {
					com.lexst.sql.column.Float col = new com.lexst.sql.column.Float(columnId);
					if(!nullable) col.setValue(float_num);
					array.add(col);
				}
				break;
			case Type.DOUBLE:
				long n_double = Numeric.toLong(block, seek, 8);
				seek += 8;
				double v_double = java.lang.Double.longBitsToDouble(n_double);
				for (int i = 0; i < itemCount; i++) {
					com.lexst.sql.column.Double col = new com.lexst.sql.column.Double(columnId);
					if(!nullable) col.setValue(v_double);
					array.add(col);
				}
				break;
			case Type.DATE:
				int v_date = Numeric.toInteger(block, seek, 4);
				seek += 4;
				for (int i = 0; i < itemCount; i++) {
					com.lexst.sql.column.Date col = new com.lexst.sql.column.Date(columnId);
					if (!nullable) col.setValue(v_date);
					array.add(col);
				}
				break;
			case Type.TIME:
				int v_time = Numeric.toInteger(block, seek, 4);
				seek += 4;
				for (int i = 0; i < itemCount; i++) {
					com.lexst.sql.column.Time col = new com.lexst.sql.column.Time(columnId);
					if (!nullable) col.setValue(v_time);
					array.add(col);
				}
				break;
			case Type.TIMESTAMP:
				long v_stamp = Numeric.toLong(block, seek, 8);
				seek += 8;
				for (int i = 0; i < itemCount; i++) {
					com.lexst.sql.column.Timestamp col = new com.lexst.sql.column.Timestamp(columnId);
					if (!nullable) col.setValue(v_stamp);
					array.add(col);
				}
				break;
			}
		}
		
		Column[] elements = new Column[array.size()];
		return array.toArray(elements);
	}
}
