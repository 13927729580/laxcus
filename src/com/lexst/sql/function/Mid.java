/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function;

import java.util.regex.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.value.*;
import com.lexst.sql.parse.*;
import com.lexst.sql.schema.*;

/**
 * @author scott.liang
 *
 */
public class Mid extends SQLFunction {
	
	private static final long serialVersionUID = -5048169558469569045L;
	
	/* MID函数格式: MID(column-name, start, [length])*/
	private final static String MID_STYLE = "^\\s*(?i)(?:MID)\\s*\\(\\s*(.+)\\s*\\)\\s*$";
	private final static String MID_TEXT1 = "^\\s*(\\w+)\\s*\\,\\s*([0-9]+)\\s*\\,\\s*([0-9]+)\\s*$";
	private final static String MID_TEXT2 = "^\\s*(\\w+)\\s*\\,\\s*([0-9]+)\\s*$";

	/* 截取字符的开始位置和截取的长度 */
	private int start, size;

	/**
	 * default
	 */
	public Mid() {
		super();
		start = size = 0;
	}

	/**
	 * @param def
	 */
	public Mid(Mid def) {
		super(def);
		this.start = def.start;
		this.size = def.size;
	}
	
	public void setStart(int i) {
		this.start = i;
	}
	public int getStart() {
		return this.start;
	}
	
	public void setLength(int i) {
		this.size = i;
	}
	public int getLength() {
		return this.size;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#duplicate()
	 */
	@Override
	public SQLFunction duplicate() {
		return new Mid(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#create(com.lexst.sql.schema.Table, java.lang.String)
	 */
	@Override
	public SQLFunction create(Table table, String sqlText) {
		Pattern pattern = Pattern.compile(Mid.MID_STYLE);
		Matcher matcher = pattern.matcher(sqlText);
		if (!matcher.matches()) {
			return null;
		}
		
		String content = matcher.group(1);
		pattern = Pattern.compile(Mid.MID_TEXT1);
		matcher = pattern.matcher(content);
		boolean match = matcher.matches();
		if (!match) {
			pattern = Pattern.compile(Mid.MID_TEXT2);
			matcher = pattern.matcher(content);
			match = matcher.matches();
		}
		if (!match) {
			throw new SQLSyntaxException("Illegal MID function: %s", sqlText);
		}
		
		String name = matcher.group(1);
		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new ColumnAttributeException("cannot find '%s'", name);
		}
		if(!attribute.isVariable()) {
			throw new ColumnAttributeException("'%s' not variable type!", name);
		}

		Mid mid = new Mid();
		mid.setReturnType(attribute.getType());
		mid.setDescription(sqlText.trim());
//		mid.setColumnId(attribute.getColumnId());
		mid.start = Integer.parseInt(matcher.group(2));
		mid.size = (matcher.groupCount() > 2 ? Integer.parseInt(matcher.group(3)) : 0);
		
		return mid;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#compute(com.lexst.sql.function.SQLValue)
	 * 
	 * 输入的参数必须是字符串类型,否则认为是错误
	 */
	@Override
	public SQLValue compute(SQLValue value) {
		if (!value.isString()) {
			throw new SQLFunctionException("cannot match! %d", value.getType());
		}

		SQLString string = (SQLString) value;
		String text = string.getValue();
		if (start > text.length()) {
			return new SQLString("");
		}

		if (size == 0) size = text.length();
		int end = start + size;
		if (end > text.length()) end = text.length();

		String sub = text.substring(start, end);

		return new SQLString( sub);
	}

}