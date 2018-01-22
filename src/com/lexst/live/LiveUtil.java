/**
 * 
 */
package com.lexst.live;

import java.util.*;

import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;

public class LiveUtil {

	public final static	long G = 1024 * 1024 * 1024;
	public final static	long M = 1024 * 1024;
	public final static	long K = 1024;

	public static String format_size(String tag, long size) {
		String s = "";
		if (size >= LiveUtil.G) {
			s = String.format("%s=%gG", tag, (double) size / (double) LiveUtil.G);
		} else if (size >= LiveUtil.M) {
			s = String.format("%s=%gM", tag, (double) size / (double) LiveUtil.M);
		} else if (size >= LiveUtil.K) {
			s = String.format("%s=%gK", tag, (double) size / (double) LiveUtil.K);
		} else {
			s = String.format("%s=%d", tag, size);
		}
		return s;
	}
	
	public static String format_size(long chunksize) {
		String s = "";
		if (chunksize >= G) s = String.format("%gG", (double) chunksize / (double) G);
		else if (chunksize >= M) s = String.format("%gM", (double) chunksize / (double) M);
		else if (chunksize >= K) s = String.format("%gK", (double) chunksize / (double) K);
		else s = String.format("%d", chunksize);
		return s;
	}

//	public static String toHex(byte[] b, int limit) {
//		StringBuilder buff = new StringBuilder();
//		for (int i = 0; i < b.length; i++) {
//			String s = String.format("%X", b[i] & 0xff);
//			if (s.length() == 1) s = "0" + s;
//			if (buff.length() == 0) buff.append("0x");
//			buff.append(s);
//			if (i + 1 >= limit) {
//				if (i + 1 < b.length) buff.append("..");
//				break;
//			}
//		}
//		return "0x" + buff.toString();
//	}
	
	
//	private static String unpacking(Table table, short columnId, Column col) {
//		ColumnAttribute attribute = table.find(columnId);
//		if (!Type.isVariable(attribute.getType())) {
//			return null;
//		}
//
//		byte[] b = null;
//		if (col.isRaw()) {
//			b = ((Raw) col).getValue();
//		} else if (col.isChar()) {
//			b = ((Char) col).getValue();
//		} else if (col.isSChar()) {
//			b = ((SChar) col).getValue();
//		} else if (col.isWChar()) {
//			b = ((WChar) col).getValue();
//		}
//
//		if (b == null || b.length == 0) {
//			return null;
//		}
//
////		VariableAttribute variable = (VariableAttribute) attribute;
////		byte[] pwd = variable.getPackingPassword();
////		switch (vf.getPacking()) {
////		case VariableAttribute.GZIP:
////			b = Inflator.gzip(b);
////			break;
////		case VariableAttribute.ZIP:
////			b = Inflator.zip(b);
////			break;
////		case VariableAttribute.AES:
////			b = SecureDecryptor.aes(pwd, b);
////			break;
////		case VariableAttribute.DES:
////			b = SecureDecryptor.des(pwd, b);
////			break;
////		case VariableAttribute.DES3:
////			b = SecureDecryptor.des3(pwd, b);
////			break;
////		case VariableAttribute.BLOWFISH:
////			b = SecureDecryptor.blowfish(pwd, b);
////			break;
////		}
//		
//		VariableAttribute variable = (VariableAttribute) attribute;
//		if(variable.getPacking().isEnabled()) {
//			b = VariableGenerator.depacking(variable, b, 0, b.length);
//		}
//
//		if (col.isRaw()) {
//			return LiveUtil.toHex(b, 16);
//		} else if (col.isChar()) {
//			return new UTF8().decode(b, 0, b.length);
//		} else if (col.isSChar()) {
//			return new UTF16().decode(b, 0, b.length);
//		} else if (col.isWChar()) {
//			return new UTF32().decode(b, 0, b.length);
//		}
//		return null;
//	}
	
//	public static String showColumn(Table table, short columnId, Column col) {
//		String value = null;
//		if (col.isRaw()) {
//			value = LiveUtil.unpacking(table, columnId, col);
//		} else if (col.isChar()) {
//			value = LiveUtil.unpacking(table, columnId, col);
//		} else if (col.isSChar()) {
//			value = LiveUtil.unpacking(table, columnId, col);
//		} else if (col.isWChar()) {
//			value = LiveUtil.unpacking(table, columnId, col);
//		} else if (col.isShort()) {
//			value = String.format("%d", ((com.lexst.sql.column.Short) col).getValue());
//		} else if (col.isInteger()) {
//			value = String.format("%d", ((com.lexst.sql.column.Integer) col).getValue());
//		} else if (col.isLong()) {
//			value = String.format("%d", ((com.lexst.sql.column.Long) col).getValue());
//		} else if (col.isFloat()) {
//			value = String.format("%f", ((com.lexst.sql.column.Float) col).getValue());
//		} else if (col.isDouble()) {
//			value = String.format("%f", ((com.lexst.sql.column.Double) col).getValue());
//		} else if (col.isDate()) {
//			int num = ((com.lexst.sql.column.Date) col).getValue();
//			java.util.Date date = com.lexst.util.datetime.SimpleDate.format(num);
//			java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
//			value = sdf.format(date);
//		} else if (col.isTime()) {
//			int num = ((com.lexst.sql.column.Time) col).getValue();
//			java.util.Date date = com.lexst.util.datetime.SimpleTime.format(num);
//			java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("HH:mm:ss SSS");
//			value = sdf.format(date);
//		} else if (col.isTimestamp()) {
//			long num = ((com.lexst.sql.column.Timestamp)col).getValue();
//			java.util.Date date = com.lexst.util.datetime.SimpleTimestamp.format(num);
//			java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
//			value = sdf.format(date);
//		}
//		
//		return value;
//	}

//	public static String showColumn(Column column) {
//		String value = null;
//		if (column.isRaw()) {
////			byte[] b = ((Raw)column).getValue();
////			if (b != null && b.length > 0) {
////				value = LiveUtil.toHex(b, 16);
////			}
//			value = ((Raw)column).toString();
//		} else if (column.isChar()) {
//			value = ((Char)column).toString();
//		} else if (column.isSChar()) {
//			value = ((SChar)column).toString();
//		} else if (column.isWChar()) {
//			value = ((WChar)column).toString();
//		} else if (column.isShort()) {
//			value = String.format("%d", ((com.lexst.sql.column.Short) column).getValue());
//		} else if (column.isInteger()) {
//			value = String.format("%d", ((com.lexst.sql.column.Integer) column).getValue());
//		} else if (column.isLong()) {
//			value = String.format("%d", ((com.lexst.sql.column.Long) column).getValue());
//		} else if (column.isFloat()) {
//			value = String.format("%g", ((com.lexst.sql.column.Float) column).getValue());
//		} else if (column.isDouble()) {
//			value = String.format("%g", ((com.lexst.sql.column.Double) column).getValue());
//		} else if (column.isDate()) {
//			int num = ((com.lexst.sql.column.Date) column).getValue();
//			java.util.Date date = com.lexst.util.datetime.SimpleDate.format(num);
//			java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
//			value = sdf.format(date);
//		} else if (column.isTime()) {
//			int num = ((com.lexst.sql.column.Time) column).getValue();
//			java.util.Date date = com.lexst.util.datetime.SimpleTime.format(num);
//			java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("HH:mm:ss SSS");
//			value = sdf.format(date);
//		} else if (column.isTimestamp()) {
//			long num = ((com.lexst.sql.column.Timestamp)column).getValue();
//			java.util.Date date = com.lexst.util.datetime.SimpleTimestamp.format(num);
//			java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
//			value = sdf.format(date);
//		}
//		return value;
//	}

	
	public static String[] showRow(Sheet sheet, Row row) {
		int size = sheet.size();
		if (size != row.size()) {
			throw new ColumnException("not match size!");
		}
		List<String> array = new ArrayList<String>(size);

		for (int index = 0; index < size; index++) {
			ColumnAttribute attribute = sheet.get(index);
			Column column = row.get(index);
			if (attribute.getType() != column.getType()) {
				throw new ColumnException("attribute not match!");
			}

			if (attribute.isRaw()) {
				String s = ((Raw) column).toString(((VariableAttribute) attribute).getPacking());
				array.add(s);
			} else if (attribute.isChar()) {
				String s = ((Char) column).toString(((VariableAttribute) attribute).getPacking());
				array.add(s);
			} else if (attribute.isSChar()) {
				String s = ((SChar) column).toString(((VariableAttribute) attribute).getPacking());
				array.add(s);
			} else if (attribute.isWChar()) {
				String s = ((WChar) column).toString(((VariableAttribute) attribute).getPacking());
				array.add(s);
			} else {
				array.add(column.toString());
			}
		}

		String[] s = new String[array.size()];
		return array.toArray(s);
	}
}