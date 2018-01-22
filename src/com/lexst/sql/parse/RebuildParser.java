/**
 * 
 */
package com.lexst.sql.parse;

import java.util.*;
import java.util.regex.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.sql.parse.result.*;
import com.lexst.sql.schema.Space;
import com.lexst.sql.schema.Table;
import com.lexst.util.host.*;

/**
 * 重构指定数据，指定表、键、和某些DATA主节点地址<br>
 * <br>
 * 语法:REBUILD schema.table [ORDER BY column] [TO address, address...]<br>
 *
 */
public class RebuildParser extends SQLParser {
	
	/** 重构表的四种语句定义 */
	private final static String REBUILD1 = "^\\s*(?i)(?:REBUILD)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:ORDER\\s+BY)\\s+(\\w+)\\s+(?i)(?:TO)\\s+(.+)$";
	private final static String REBUILD2 = "^\\s*(?i)(?:REBUILD)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:ORDER\\s+BY)\\s+(\\w+)\\s*$";
	private final static String REBUILD3 = "^\\s*(?i)(?:REBUILD)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:TO)\\s+(.+)$";
	private final static String REBUILD4 = "^\\s*(?i)(?:REBUILD)\\s+(\\w+)\\.(\\w+)\\s*$";

	/**
	 * default
	 */
	public RebuildParser() {
		super();
	}
	
	/**
	 * 解析REBUILD语句，返回一个类对象
	 * 
	 * @param sql
	 * @param table
	 * @return
	 */
	public RebuildHostResult split(String sql, SQLChooser chooser) {
		Space space = null;
		String columnName = null;
		String hosts = null;

		Pattern pattern = Pattern.compile(RebuildParser.REBUILD1);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (match) {
			space = new Space(matcher.group(1), matcher.group(2));
			columnName = matcher.group(3);
			hosts = matcher.group(4);
		}
		if(!match) {
			pattern = Pattern.compile(RebuildParser.REBUILD2);
			matcher = pattern.matcher(sql);
			if(match = matcher.matches()) {
				space = new Space(matcher.group(1), matcher.group(2));
				columnName = matcher.group(3);
			}
		}
		if(!match) {
			pattern = Pattern.compile(RebuildParser.REBUILD3);
			matcher = pattern.matcher(sql);
			if(match = matcher.matches()) {
				space = new Space(matcher.group(1), matcher.group(2));
				hosts = matcher.group(3);
			}
		}
		if(!match) {
			pattern = Pattern.compile(RebuildParser.REBUILD4);
			matcher = pattern.matcher(sql);
			if(match = matcher.matches()) {
				space = new Space(matcher.group(1), matcher.group(2));
			}
		}
		if (!match) {
			throw new SQLSyntaxException("syntax error or missing!");
		}
		
		// 查找对应的数据库表配置
		Table table = chooser.findTable(space);
		if (table == null) {
			throw new SQLSyntaxException("cannot find %s", space);
		}
		
		RebuildHostResult host = new RebuildHostResult(space);
		// 解析索引
		if (columnName != null) {
			ColumnAttribute attribute = table.find(columnName);
			if (attribute == null) {
				throw new SQLSyntaxException("cannot find:%s", columnName);
			}
			// 如果是行存储模型，必须是索引键
			if (table.isNSM() && !attribute.isKey()) {
				throw new SQLSyntaxException("invalid key:%s", columnName);
			}
			host.setColumnId(attribute.getColumnId());
		}
		// 解析指定的DATA节点地址
		if (hosts != null) {
			List<Address> list = splitIP(hosts);
			host.addAddresses(list);

//			List<String> list = super.splitIP(hosts);
//			host.addAllIP(list);
		}
		return host;
	}
		
}