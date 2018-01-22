/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function;

import com.lexst.sql.charset.*;
import com.lexst.sql.function.value.*;

/**
 * @author scott.liang
 * 
 */
public class SQLFunctionComputer {

	/**
	 * default
	 */
	public SQLFunctionComputer() {
		super();
	}

	public static short toShort(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isShort()) {
			return ((SQLShort)result).getValue();
		}
		return 0;
	}

	public static com.lexst.sql.column.Short toShort(short columnId,
			SQLFunction function, SQLValue value) {
		short num = toShort(function, value);
		return new com.lexst.sql.column.Short(columnId, num);
	}

	public static int toInteger(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isInteger()) {
			return ((SQLInteger)result).getValue();
		}
		return 0;
	}

	public static com.lexst.sql.column.Integer toInteger(short columnId,
			SQLFunction function, SQLValue value) {
		int num = toInteger(function, value);
		return new com.lexst.sql.column.Integer(columnId, num);
	}

	public static long toLong(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isLong()) {
			return ((SQLong) result).getValue();
		}
		return -1L;
	}

	public static com.lexst.sql.column.Long toLong(short columnId,
			SQLFunction function, SQLValue value) {
		long num = toLong(function, value);
		return new com.lexst.sql.column.Long(columnId, num);
	}

	public static float toFloat(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isFloat()) {
			return ((SQLFloat)result).getValue();
		}
		return 0.0f;
	}

	public static double toDouble(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isDouble()) {
			return ((SQLDouble)result).getValue();
		}
		return 0.0f;
	}

	public static int toDate(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isDate()) {
			return ((SQLDate)result).getValue();
		}
		return -1;
	}

	public static int toTime(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isTime()) {
			return ((SQLTime)result).getValue();
		}
		return -1;
	}

	public static long toTimestamp(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isTimestamp()) {
			SQLTimestamp ref = (SQLTimestamp) result;
			return ref.getValue();
//			java.util.Date date = com.lexst.util.datetime.SimpleTimestamp.format(ref.getValue());
//			return com.lexst.util.datetime.SimpleTimestamp.format(date);
		}
		return -1L; // INVALID VALUE
	}

	public static byte[] toRaw(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isRaw()) {
			return ((SQLRaw)result).getValue();
		}
		return null;
	}

	public static com.lexst.sql.column.Raw toRaw(short columnId,
			SQLFunction function, SQLValue value) {
		byte[] b = toRaw(function, value);
		return new com.lexst.sql.column.Raw(columnId, b);
	}

	public static byte[] toChar(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isString()) {
			return new UTF8().encode(((SQLString) result).getValue());
		}
		return null;
	}

	public static com.lexst.sql.column.Char toChar(short columnId,
			SQLFunction function, SQLValue value) {
		byte[] b = toChar(function, value);
		return new com.lexst.sql.column.Char(columnId, b);
	}

	public static byte[] toSChar(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isString()) {
			return new UTF16().encode(((SQLString) result).getValue());
		}
		return null;
	}

	public static com.lexst.sql.column.SChar toSChar(short columnId,
			SQLFunction function, SQLValue value) {
		byte[] b = toSChar(function, value);
		return new com.lexst.sql.column.SChar(columnId, b);
	}

	public static byte[] toWChar(SQLFunction function, SQLValue value) {
		SQLValue result = function.compute(value);
		if (result.isString()) {
			return new UTF32().encode(((SQLString) result).getValue());
		}
		return null;
	}

	public static com.lexst.sql.column.WChar toWChar(short columnId,
			SQLFunction function, SQLValue value) {
		byte[] b = toWChar(function, value);
		return new com.lexst.sql.column.WChar(columnId, b);
	}

}