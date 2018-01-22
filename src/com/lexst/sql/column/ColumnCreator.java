/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.*;

/**
 * @author scott.liang
 *
 */
public class ColumnCreator {

	/**
	 * 
	 */
	public ColumnCreator() {
		super();
	}

	public static Column create(byte type) {
		Column column = null;
		switch (type) {
		case Type.RAW:
			column = new com.lexst.sql.column.Raw();
			break;
		case Type.CHAR:
			column = new com.lexst.sql.column.Char();
			break;
		case Type.VCHAR:
			column = new com.lexst.sql.column.VChar();
			break;			
		case Type.SCHAR:
			column = new com.lexst.sql.column.SChar();
			break;
		case Type.VSCHAR:
			column = new com.lexst.sql.column.VSChar();
			break;			
		case Type.WCHAR:
			column = new com.lexst.sql.column.WChar();
			break;
		case Type.VWCHAR:
			column = new com.lexst.sql.column.VWChar();
			break;
		case Type.SHORT:
			column = new com.lexst.sql.column.Short();
			break;
		case Type.INTEGER:
			column = new com.lexst.sql.column.Integer();
			break;
		case Type.LONG:
			column = new com.lexst.sql.column.Long();
			break;
		case Type.FLOAT:
			column = new com.lexst.sql.column.Float();
			break;
		case Type.DOUBLE:
			column = new com.lexst.sql.column.Double();
			break;
		case Type.DATE:
			column = new com.lexst.sql.column.Date();
			break;
		case Type.TIME:
			column = new com.lexst.sql.column.Time();
			break;
		case Type.TIMESTAMP:
			column = new com.lexst.sql.column.Timestamp();
			break;
		}
		return column;
	}

}
