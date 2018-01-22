/**
 * 
 */
package com.lexst.sql.parse;

import java.util.*;
import java.util.regex.*;

import com.lexst.site.*;
import com.lexst.sql.parse.result.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;

public class ChunkParser extends SQLParser {
	
	/** 从磁盘上加载指定的数据块到内存中，格式: LOAD CHUNK schema.table [TO ip-address,...]**/
	private final static String LOAD_CHUNK1 = "^\\s*(?i)(?:LOAD\\s+CHUNK)\\s+(\\w+)\\.(\\w+)\\s*$";
	private final static String LOAD_CHUNK2 = "^\\s*(?i)(?:LOAD\\s+CHUNK)\\s+(\\w+)\\.(\\w+)\\s+(?i)TO\\s+(.+?)\\s*$";
	
	/** 从内存中卸载指定的数据块内容，格式: STOP CHUNK|UNLOAD CHUNK schema.table [FROM host_address,...] **/
	private final static String STOP_CHUNK1 = "^\\s*(?i)(?:STOP\\s+CHUNK|UNLOAD\\s+CHUNK)\\s+(\\w+)\\.(\\w+)\\s*$";
	private final static String STOP_CHUNK2 = "^\\s*(?i)(?:STOP\\s+CHUNK|UNLOAD\\s+CHUNK)\\s+(\\w+)\\.(\\w+)\\s+(?i)FROM\\s+(.+?)\\s*$";

	/** 设置数据块的尺寸，单位：兆，语法格式: SET CHUNKSIZE schema.table digitM */
	private final static String SET_CHUNKSIZE = "^\\s*(?i)(?:SET\\s+CHUNKSIZE)\\s+(\\w+)\\.(\\w+)\\s+([0-9]{1,})(?i)M\\s*$";

	/** 显示数据库或者数据表的数据块尺寸，语法格式: SHOW CHUNKSIZE schema[.table] [FROM DATASITE|HOMESITE ip_address, ...] */
	private final static String SHOW_CHUNKSIZE1 = "^\\s*(?i)(?:SHOW\\s+CHUNKSIZE)\\s+(\\w+)\\.(\\w+)\\s+(?i)FROM\\s+(?i)(DATASITE|HOMESITE)\\s+(.+)\\s*$";
	private final static String SHOW_CHUNKSIZE2 = "^\\s*(?i)(?:SHOW\\s+CHUNKSIZE)\\s+(\\w+)\\s+(?i)FROM\\s+(?i)(DATASITE|HOMESITE)\\s+(.+)\\s*$";
	private final static String SHOW_CHUNKSIZE3 = "^\\s*(?i)(?:SHOW\\s+CHUNKSIZE)\\s+(\\w+)\\.(\\w+)\\s*$";
	private final static String SHOW_CHUNKSIZE4 = "^\\s*(?i)(?:SHOW\\s+CHUNKSIZE)\\s+(\\w+)\\s*$";
	
	/**
	 * 
	 */
	public ChunkParser() {
		super();
	}
	
	/**
	 * 解析加载数据块
	 * 
	 * @param sql - 格式: LOAD CHUNK schema.table [TO address, address, ...]
	 * @return
	 */
	public RebuildHostResult splitLoadChunk(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(ChunkParser.LOAD_CHUNK2);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (!match) {
			pattern = Pattern.compile(ChunkParser.LOAD_CHUNK1);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
		}
		if (!match) {
			throw new SQLSyntaxException("syntax error or missing!");
		}

		Space space = new Space(matcher.group(1), matcher.group(2));
		if(online){
			if (!chooser.onTable(space)) {
				throw new SQLSyntaxException("cannot find %s", space);
			}
		}
		RebuildHostResult host = new RebuildHostResult(space);

		if (matcher.groupCount() > 2) {
			String suffix = matcher.group(3);
			List<Address> list = splitIP(suffix);
			host.addAddresses(list);
		}

		return host;
	}

	/**
	 * 解析卸载数据
	 * 
	 * @param sql - STOP CHUNK schema.table [FROM address, address, ...]
	 * @return
	 */
	public RebuildHostResult splitStopChunk(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(ChunkParser.STOP_CHUNK2);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (!match) {
			pattern = Pattern.compile(ChunkParser.STOP_CHUNK1);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
		}
		if (!match) {
			throw new SQLSyntaxException("syntax error or missing!");
		}

		Space space = new Space(matcher.group(1), matcher.group(2));
		if(online) {
			if (!chooser.onTable(space)) {
				throw new SQLSyntaxException("cannot find %s", space);
			}
		}
		RebuildHostResult host = new RebuildHostResult(space);

		if (matcher.groupCount() > 2) {
			String suffix = matcher.group(3);
			List<Address> list = splitIP(suffix);
			host.addAddresses(list);
		}
		return host;
	}

//	private RebuildHost splitLoadChunk2(String sql) {
//		Pattern pattern = Pattern.compile(ChunkParser.LOAD_CHUNK2);
//		Matcher matcher = pattern.matcher(sql);
//		if (!matcher.matches()) return null;
//
//		String schema = matcher.group(1);
//		String table = matcher.group(2);
//		RebuildHost host = new RebuildHost(schema, table);
//		
//		String suffix = matcher.group(3);
//		List<String> ip_list = splitIP(suffix);
//		if(ip_list != null) {
//			host.addAllIP(ip_list);
//		}
//		return host;
//	}
//	
//	private RebuildHost splitStopChunk2(String sql) {
//		Pattern pattern = Pattern.compile(ChunkParser.STOP_CHUNK2);
//		Matcher matcher = pattern.matcher(sql);
//		if (!matcher.matches()) return null;
//
//		String schema = matcher.group(1);
//		String table = matcher.group(2);
//		RebuildHost host = new RebuildHost(schema, table);
//		
//		String suffix = matcher.group(3);
//		List<String> list = splitIP(suffix);
//		if(list != null) {
//			host.addAllIP(list);
//		}
//		return host;
//	}
//
//	public RebuildHost splitLoadChunk(String sql) {
//		Pattern pattern = Pattern.compile(ChunkParser.LOAD_CHUNK1);
//		Matcher matcher = pattern.matcher(sql);
//		if (!matcher.matches()) {
//			return splitLoadChunk2(sql);
//		}
//		String schema = matcher.group(1);
//		String table = matcher.group(2);
//		return new RebuildHost(schema, table);
//	}
//	
//	public RebuildHost splitStopChunk(String sql) {
//		Pattern pattern = Pattern.compile(ChunkParser.STOP_CHUNK1);
//		Matcher matcher = pattern.matcher(sql);
//		if (!matcher.matches()) {
//			return splitStopChunk2(sql);
//		}
//		String schema = matcher.group(1);
//		String table = matcher.group(2);
//		return new RebuildHost(schema, table);
//	}

	private int alterSite(String site) {
		if("datasite".equalsIgnoreCase(site)) {
			return Site.DATA_SITE;
		} else if("homesite".equalsIgnoreCase(site)) {
			return Site.HOME_SITE;
		} 
		return 0;
	}

	public ChunkHostResult splitShowChunkSize(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(ChunkParser.SHOW_CHUNKSIZE1);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			Space space = new Space(matcher.group(1), matcher.group(2));
			if (online) {
				if (!chooser.onTable(space)) {
					throw new SQLSyntaxException("cannot find %s", space);
				}
			}
			int siteType = alterSite(matcher.group(3));
			String site_ip = matcher.group(4);
			List<Address> ip_list = this.splitIP(site_ip);
			return new ChunkHostResult(space, siteType, ip_list);
		}

		pattern = Pattern.compile(ChunkParser.SHOW_CHUNKSIZE2);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String schema = matcher.group(1);
			if (online) {
				if (!chooser.onSchema(schema)) {
					throw new SQLSyntaxException("cannot find %s", schema);
				}
			}
			int site_type = alterSite(matcher.group(2));
			String site_ip = matcher.group(3);
			List<Address> ip_list = this.splitIP(site_ip);
			return new ChunkHostResult(schema, site_type, ip_list);
		}

		pattern = Pattern.compile(ChunkParser.SHOW_CHUNKSIZE3);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			Space space = new Space(matcher.group(1), matcher.group(2));
			if (online) {
				if (!chooser.onTable(space)) {
					throw new SQLSyntaxException("cannot find %s", space);
				}
			}
			return new ChunkHostResult(space);
		}

		pattern = Pattern.compile(ChunkParser.SHOW_CHUNKSIZE4);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String schema = matcher.group(1);
			if (online) {
				if (!chooser.onSchema(schema)) {
					throw new SQLSyntaxException("cannot find %s", schema);
				}
			}
			return new ChunkHostResult(schema);
		}

		throw new SQLSyntaxException("syntax error or missing!");
	}

	/**
	 * 解析 数据块尺寸定义
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Object[] splitSetChunkSize(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(ChunkParser.SET_CHUNKSIZE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("sql error or missing!");
		}

		Space space = new Space(matcher.group(1), matcher.group(2));
		if(online) {
			if (!chooser.onTable(space)) {
				throw new SQLSyntaxException("cannot find %s", space);
			}
		}

		String digit = matcher.group(3);
		int size = Integer.parseInt(digit) * SQLParser.m;
		return new Object[] { space, new Integer(size) };
	}

}