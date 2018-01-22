/**
 * 
 */
package com.lexst.sql.parse;

import java.util.regex.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.sql.schema.*;
import com.lexst.util.datetime.*;

/**
 * 解析表数据重构时间<br>
 * 此操作产生在终端上，解析数据发向TOP节点，TOP节点记录后定时检查参数，达到触发时间通知HOME节点，HOME节点再通知DATA节点进行重构<br>
 */
public class SwitchTimeParser extends SQLParser {

	/** 重构语句格式 */
	private final static String SWITCH_SPACE = "^\\s*(?i)(?:SET\\s+SWITCH\\s+TIME)\\s+(\\w+)\\.(\\w+)\\s+(.+)$";
	private final static String SWITCH_TIME1 = "^\\s*(?i)(?:SET\\s+SWITCH\\s+TIME)\\s+(\\w+)\\.(\\w+)\\s+(?i)(HOURLY|DAILY|WEEKLY|MONTHLY)\\s+\\'(.+)\\'\\s+(?i)(?:ORDER\\s+BY)\\s+(\\w+)\\s*$";
	private final static String SWITCH_TIME2 = "^\\s*(?i)(?:SET\\s+SWITCH\\s+TIME)\\s+(\\w+)\\.(\\w+)\\s+(?i)(HOURLY|DAILY|WEEKLY|MONTHLY)\\s+\\'(.+)\\'\\s*$";

	/** 每小时更新(分:秒) */
	private final static String SQL_HOURLY = "^\\s*([0-9]{1,2}):([0-9]{1,2})\\s*$";

	/** 每日更新(时:分:秒) */
	private final static String SQL_DAILY = "^\\s*([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})\\s*$";

	/** 每周更新(天 时:分:秒) */
	private final static String SQL_WEEKLY = "^\\s*([0-9]{1})\\s+([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})\\s*$";

	/** 每个月更新(天 时:分:秒) */
	private final static String SQL_MONTHLY = "^\\s*([0-9]{1,2})\\s+([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})\\s*$";

	/**
	 * 
	 */
	public SwitchTimeParser() {
		super();
	}
	
	/**
	 * 从语句中提取表名
	 * 
	 * @param sql
	 * @return
	 */
	public Space splitSpace(String sql) {
		Pattern pattern = Pattern.compile(SwitchTimeParser.SWITCH_SPACE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			return null;
		}
		
		String schema = matcher.group(1);
		String table = matcher.group(2);
		return new Space(schema, table);
	}

	/**
	 * 解析数据重构时间
	 * 
	 * @param sql
	 * @return
	 */
	public SwitchTime split(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SwitchTimeParser.SWITCH_TIME1);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (!match) {
			pattern = Pattern.compile(SwitchTimeParser.SWITCH_TIME2);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
		}
		if (!match) {
			throw new SQLSyntaxException("syntax error or missing:%s", sql);
		}

		// 取数据库表名称
		Space space = new Space(matcher.group(1), matcher.group(2));
		Table table = chooser.findTable(space);
		if (table == null) {
			throw new SQLSyntaxException("cannot find %s", space);
		}
		
		String option = matcher.group(3);
		String style = matcher.group(4);

		SwitchTime switchTime = new SwitchTime(space);
		
		// 取排列名
		if (matcher.groupCount() > 4) {
			String name = matcher.group(5);
			ColumnAttribute attribute = table.find(name);
			if (attribute == null) {
				throw new SQLSyntaxException("cannot find:%s", name);
			}
			// 如果是行存储模型，必须是索引键
			if (table.isNSM() && !attribute.isKey()) {
				throw new SQLSyntaxException("cannot set key:%s", name);
			}
			switchTime.setColumnId(attribute.getColumnId());
		}
		
		// 解析触发时间
		if ("HOURLY".equalsIgnoreCase(option)) {
			pattern = Pattern.compile(SwitchTimeParser.SQL_HOURLY);
			matcher = pattern.matcher(style);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("illegal hourly:%s", style);
			}
			
			int minute = Integer.parseInt(matcher.group(1));
			int second = Integer.parseInt(matcher.group(2));
			if (minute > 23 || second > 59) {
				throw new SQLSyntaxException("illegal hourly:%s", style);
			}
			long interval = SimpleTimestamp.format(0, 0, 0, 0, minute, second, 0);
			switchTime.setInterval(interval);
			switchTime.setType(SwitchTime.HOURLY);
		} else if ("DAILY".equalsIgnoreCase(option)) {
			pattern = Pattern.compile(SwitchTimeParser.SQL_DAILY);
			matcher = pattern.matcher(style);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("illegal daily:%s", style);
			}
			
			int hour = Integer.parseInt(matcher.group(1));
			int minute = Integer.parseInt(matcher.group(2));
			int second = Integer.parseInt(matcher.group(3));
			if (hour > 23 || minute > 59 || second > 59) {
				throw new SQLSyntaxException("illegal hourly:%s", style);
			}
			long interval = SimpleTimestamp.format(0, 0, 0, hour, minute, second, 0);
			switchTime.setInterval(interval);
			switchTime.setType( SwitchTime.DAILY);
		} else if ("WEEKLY".equalsIgnoreCase(option)) {
			pattern = Pattern.compile(SwitchTimeParser.SQL_WEEKLY);
			matcher = pattern.matcher(style);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("illegal weekly:%s", style);
			}
			
			int day = Integer.parseInt(matcher.group(1));
			int hour = Integer.parseInt(matcher.group(2));
			int minute = Integer.parseInt(matcher.group(3));
			int second = Integer.parseInt(matcher.group(4));
			if (day < 1 || day > 7 || hour > 23 || minute > 59 || second > 59) {
				throw new SQLSyntaxException("illegal weekly:%s", style);
			}
			long interval = SimpleTimestamp.format(0, 0, day, hour, minute, second, 0);
			switchTime.setInterval(interval);
			switchTime.setType(SwitchTime.WEEKLY);
		} else if ("MONTHLY".equalsIgnoreCase(option)) {
			pattern = Pattern.compile(SwitchTimeParser.SQL_MONTHLY);
			matcher = pattern.matcher(style);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("illegal monthly:%s", style);
			}

			int day = Integer.parseInt(matcher.group(1));
			int hour = Integer.parseInt(matcher.group(2));
			int minute = Integer.parseInt(matcher.group(3));
			int second = Integer.parseInt(matcher.group(4));
			if (day > 31 || hour > 23 || minute > 59 || second > 59) {
				throw new SQLSyntaxException("illegal monthly:%s", style);
			}
			long interval = SimpleTimestamp.format(0, 0, day, hour, minute, second, 0);
			switchTime.setInterval(interval);
			switchTime.setType(SwitchTime.MONTHLY);
		}
		
		return switchTime;
	}
	
	/**
	 * 解析重构语句参数
	 * 
	 * @param sql
	 * @return
	 */
	public SwitchTime split2(String sql, Table table) {
		SwitchTime switchTime = new SwitchTime();

		Pattern pattern = Pattern.compile(SwitchTimeParser.SWITCH_TIME1);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (match) {
			String name = matcher.group(5);
			ColumnAttribute attribute = table.find(name);
			if (attribute == null) {
				throw new SQLSyntaxException("cannot find:%s", name);
			}
			// 如果是行存储模型，必须是索引键
			if (table.isNSM() && !attribute.isKey()) {
				throw new SQLSyntaxException("cannot set key:%s", name);
			}
			switchTime.setColumnId(attribute.getColumnId());
		}
		if (!match) {
			pattern = Pattern.compile(SwitchTimeParser.SWITCH_TIME2);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
		}
		if (!match) {
			throw new SQLSyntaxException("invalid rebuild syntax:%s", sql);
		}

		Space space = new Space(matcher.group(1), matcher.group(2));
		
		switchTime.setSpace(space);
		String option = matcher.group(3);
		String time = matcher.group(4);
		
		if ("HOURLY".equalsIgnoreCase(option)) {
			pattern = Pattern.compile(SwitchTimeParser.SQL_HOURLY);
			matcher = pattern.matcher(time);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("illegal hourly:%s", time);
			}
			
			int minute = Integer.parseInt(matcher.group(1));
			int second = Integer.parseInt(matcher.group(2));
			if (minute > 23 || second > 59) {
				throw new SQLSyntaxException("illegal hourly:%s", time);
			}
			long interval = SimpleTimestamp.format(0, 0, 0, 0, minute, second, 0);
			switchTime.setInterval(interval);
			switchTime.setType(SwitchTime.HOURLY);
		} else if ("DAILY".equalsIgnoreCase(option)) {
			pattern = Pattern.compile(SwitchTimeParser.SQL_DAILY);
			matcher = pattern.matcher(time);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("illegal daily:%s", time);
			}
			
			int hour = Integer.parseInt(matcher.group(1));
			int minute = Integer.parseInt(matcher.group(2));
			int second = Integer.parseInt(matcher.group(3));
			if (hour > 23 || minute > 59 || second > 59) {
				throw new SQLSyntaxException("illegal hourly:%s", time);
			}
			long interval = SimpleTimestamp.format(0, 0, 0, hour, minute, second, 0);
			switchTime.setInterval(interval);
			switchTime.setType( SwitchTime.DAILY);
		} else if ("WEEKLY".equalsIgnoreCase(option)) {
			pattern = Pattern.compile(SwitchTimeParser.SQL_WEEKLY);
			matcher = pattern.matcher(time);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("illegal weekly:%s", time);
			}
			
			int day = Integer.parseInt(matcher.group(1));
			int hour = Integer.parseInt(matcher.group(2));
			int minute = Integer.parseInt(matcher.group(3));
			int second = Integer.parseInt(matcher.group(4));
			if (day < 1 || day > 7 || hour > 23 || minute > 59 || second > 59) {
				throw new SQLSyntaxException("illegal weekly:%s", time);
			}
			long interval = SimpleTimestamp.format(0, 0, day, hour, minute, second, 0);
			switchTime.setInterval(interval);
			switchTime.setType(SwitchTime.WEEKLY);
		} else if ("MONTHLY".equalsIgnoreCase(option)) {
			pattern = Pattern.compile(SwitchTimeParser.SQL_MONTHLY);
			matcher = pattern.matcher(time);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("illegal monthly:%s", time);
			}

			int day = Integer.parseInt(matcher.group(1));
			int hour = Integer.parseInt(matcher.group(2));
			int minute = Integer.parseInt(matcher.group(3));
			int second = Integer.parseInt(matcher.group(4));
			if (day > 31 || hour > 23 || minute > 59 || second > 59) {
				throw new SQLSyntaxException("illegal monthly:%s", time);
			}
			long interval = SimpleTimestamp.format(0, 0, day, hour, minute, second, 0);
			switchTime.setInterval(interval);
			switchTime.setType(SwitchTime.MONTHLY);
		}
		
		return switchTime;
	}

}