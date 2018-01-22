/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.column.attribute;

import com.lexst.sql.*;

/**
 * @author scott.liang
 * 
 */
public class ColumnAttributeCreator {

	/**
	 * 
	 */
	public ColumnAttributeCreator() {
		super();
	}

	/**
	 * 根据关键字，返回对应的列属性实例
	 * 
	 * @param keyword
	 * @return
	 */
	public static ColumnAttribute create(String keyword) {
		ColumnAttribute attribute = null;
		if ("RAW".equalsIgnoreCase(keyword)
				|| "BINARY".equalsIgnoreCase(keyword)) {
			attribute = new RawAttribute();
		} else if ("CHAR".equalsIgnoreCase(keyword)) {
			attribute = new CharAttribute();
		} else if ("SCHAR".equalsIgnoreCase(keyword)) {
			attribute = new SCharAttribute();
		} else if ("WCHAR".equalsIgnoreCase(keyword)) {
			attribute = new WCharAttribute();
		} else if ("SHORT".equalsIgnoreCase(keyword)
				|| "SMALLINT".equalsIgnoreCase(keyword)) {
			attribute = new ShortAttribute();
		} else if ("INTEGER".equalsIgnoreCase(keyword)
				|| "INT".equalsIgnoreCase(keyword)) {
			attribute = new IntegerAttribute();
		} else if ("LONG".equalsIgnoreCase(keyword)
				|| "BIGINT".equalsIgnoreCase(keyword)) {
			attribute = new LongAttribute();
		} else if ("REAL".equalsIgnoreCase(keyword)
				|| "FLOAT".equalsIgnoreCase(keyword)) {
			attribute = new FloatAttribute();
		} else if ("DOUBLE".equalsIgnoreCase(keyword)) {
			attribute = new DoubleAttribute();
		} else if ("DATE".equalsIgnoreCase(keyword)) {
			attribute = new DateAttribute();
		} else if ("TIME".equalsIgnoreCase(keyword)) {
			attribute = new TimeAttribute();
		} else if ("TIMESTAMP".equalsIgnoreCase(keyword)
				|| "DATETIME".equalsIgnoreCase(keyword)) {
			attribute = new TimestampAttribute();
		}

		return attribute;
	}

	/**
	 * 根据列属性类型，生成属性对象
	 * @param type
	 * @return
	 */
	public static ColumnAttribute create(byte type) {
		ColumnAttribute attribute = null;
		switch (type) {
		case Type.RAW:
			attribute = new RawAttribute();
			break;
		case Type.CHAR:
			attribute = new CharAttribute();
			break;
		case Type.SCHAR:
			attribute = new SCharAttribute();
			break;
		case Type.WCHAR:
			attribute = new WCharAttribute();
			break;
		case Type.SHORT:
			attribute = new ShortAttribute();
			break;
		case Type.INTEGER:
			attribute = new IntegerAttribute();
			break;
		case Type.LONG:
			attribute = new LongAttribute();
			break;
		case Type.FLOAT:
			attribute = new FloatAttribute();
			break;
		case Type.DOUBLE:
			attribute = new DoubleAttribute();
			break;
		case Type.DATE:
			attribute = new DateAttribute();
			break;
		case Type.TIME:
			attribute = new TimeAttribute();
			break;
		case Type.TIMESTAMP:
			attribute = new TimestampAttribute();
			break;
		}
		return attribute;
	}
	

}