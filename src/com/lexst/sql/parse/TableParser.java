/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * "create table, drop table" syntax parser
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 6/30/2009
 * 
 * @see com.lexst.sql.parse
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.parse;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

import com.lexst.sql.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.util.*;
import com.lexst.util.host.*;


/**
 * 建表格式
 * 
	CREATE TABLE
		[SM|STORAGEMODEL=DSM|NSM|COLUMNS|ROWS]
		[HOSTMODEL|HM=SHARE|EXCLUSIVE]
		[HOSTCACHE=YES|NO]
		[CHUNKSIZE={digit}M]
		[CHUNKCOPY={digit}]
		[PRIMEHOSTS={digit}]
		[CLUSTERS={digit}]
	DATABASE-NAME.TABLE-NAME
	(
		[COLUMN-NAME COLUMN-TYPE 
			[NOT NULL|NULL]
			[NOT CASE|CASE]
			[NOT LIKE|LIKE]
			[PRIME KEY|SLAVE KEY[(digit)]]
			[DEFAULT [{string}]|[{digit}]|[{function description}]]
			[PACKING [encrypt-name:'{password}'] AND [compress-name]]
			[CHECK ({description}]
		]
		
		[COLUMN-NAME COLUMN-DATATYPE ...]
	}
 *
 */
public class TableParser extends SQLParser {

	/** 建表格式: create table database.table (id int, word char not null like not case default 'pentium', tag long) **/
	private final static String CREATE_TABLE1 = "^\\s*(?i)CREATE\\s+(?i)TABLE\\s+([\\p{Print}]+)\\s+([\\w]+)\\.([\\w]+)\\s*\\((.+)\\)\\s*$";
	private final static String CREATE_TABLE2 = "^\\s*(?i)CREATE\\s+(?i)TABLE\\s+([\\w]+)\\.([\\w]+)\\s*\\((.+)\\)\\s*$";

	/** 建表时的配置参数集  **/
	private final static String TABLE_PREFIX_STORAGEMODEL = "^\\s*(?i)(?:SM|STORAGEMODEL)\\s*=\\s*(?i)(DSM|COLUMNS|NSM|ROWS)(\\s*|\\s+.+)$";
	private final static String TABLE_PREFIX_PRIMEHOSTS = "^\\s*(?i)PRIMEHOSTS\\s*=\\s*(\\d+)(\\s*|\\s+.+)$";
	private final static String TABLE_PREFIX_HOSTMODE = "^\\s*(?i)(?:HM|HOSTMODE)\\s*=\\s*(?i)(SHARE|EXCLUSIVE)(\\s*|\\s+.+)$";
	private final static String TABLE_PREFIX_CHUNKSIZE = "^\\s*(?i)CHUNKSIZE\\s*=\\s*(\\d+)(?i)M(\\s*|\\s+.+)$";
	private final static String TABLE_PREFIX_CHUNKCOPY = "^\\s*(?i)CHUNKCOPY\\s*=\\s*(\\d+)(\\s*|\\s+.+)$";
	private final static String TABLE_PREFIX_HOSTCACHE = "^\\s*(?i)HOSTCACHE\\s*=\\s*(?i)(YES|NO)(\\s*|\\s+.+)$";
	private final static String TABLE_PREFIX_CLUSTERS_NUMBER = "^\\s*(?i)(?:CLUSTERS|HOMES)\\s*=\\s*(\\d+)(\\s*|\\s+.+)$";
	private final static String TABLE_PREFIX_CLUSTERS_ADDRESS = "^\\s*(?i)(?:CLUSTERS|HOMES)\\s*=\\s*\\'([\\w\\d\\.\\,\\s]+)\\'(\\s*|\\s+.+)$";
	
	/** 列的基本属性， NULL|NOT NULL, CASE|NOT CASE, LIKE|NOT LIKE. 后两项只限可变长类型 **/
	private final static String TABLE_COLUMN_NULLORNOT = "^\\s*(?i)(NOT\\s+NULL|NULL)(\\s+.+|\\s*)$";
	private final static String TABLE_COLUMN_CASEORNOT = "^\\s*(?i)(NOT\\s+CASE|CASE)(\\s+.+|\\s*)$";
	private final static String TABLE_COLUMN_LIKEORNOT = "^\\s*(?i)(NOT\\s+LIKE|LIKE)(\\s+.+|\\s*)$";

	/** PAKCING. 压缩和加密. 格式: packing des:'unix' | packing gzip | packing gzip and des:'unix' | packing des:'unix' and gzip **/
	private final static String ENCRYPT_COMPRESS = "^\\s*(?i)PACKING\\s+(\\w+)\\s*\\:\\s*\\'(\\p{Graph}+)\\'\\s+(?i)(?:AND)\\s+(\\w+)(\\s+.+|\\s*)$"; // 加密和压缩
	private final static String COMPRESS_ENCRYPT = "^\\s*(?i)PACKING\\s+(\\w+)\\s+(?i)(?:AND)\\s+(\\w+)\\s*\\:\\s*\\'(\\p{Graph}+)\\'(\\s+.+|\\s*)$"; // 压缩和加密
	private final static String ENCRYPT = "^\\s*(?i)PACKING\\s+(\\w+)\\s*\\:\\s*\\'(\\p{Graph}+)\\'(\\s+.+|\\s*)$";  // 加密
	private final static String COMPRESS = "^\\s*(?i)PACKING\\s+(\\w+)(\\s+.+|\\s*)$"; // 压缩

	/** 键判断. 格式: PRIME KEY|SLAVE KEY [(20)]. 可变长类型列需要指定长度，默认是16字节 **/
	private final static String INDEXKEY_LIMIT = "^\\s*(?i)(PRIME\\s+KEY|SLAVE\\s+KEY)\\s*\\(\\s*([0-9]+)\\s*\\)(\\s+.+|\\s*)$";
	private final static String INDEXKEY = "^\\s*(?i)(PRIME\\s+KEY|SLAVE\\s+KEY)(\\s+.+|\\s*)$";

	/** 默认值判断，包括字符串,整数值,浮点值,函数 **/
	private final static String DEFAULT_VARIABLE = "^\\s*(?i)DEFAULT\\s+\\'(.+?)\\'(\\s+.+|\\s*)$";
	private final static String DEFAULT_DIGIT_FLOAT = "^\\s*(?i)DEFAULT\\s+([-+]?\\d+\\.\\d+)(\\s+.+|\\s*)$";
	private final static String DEFAULT_DIGIT_CONST = "^\\s*(?i)DEFAULT\\s+([-+]?\\d+)(\\s+.+|\\s*)$";
	private final static String DEFAULT_FUNCTION = "^\\s*(?i)DEFAULT\\s+(\\w+\\s*\\(\\s*.*\\s*\\))(\\s+.+|\\s*)$";

	/** RAW|BINARY|CHAR|SCHAR|WCHAR|SHORT|SMALLINT|INT|INTEGER|LONG|BIGINT|REAL|FLOAT|DOUBLE|TIMESTAMP|DATE|TIME **/
	/** 列类型格式 **/
	private final static String TABLE_COLUMN = "^\\s*(\\w+)\\s+(?i)(RAW|BINARY|CHAR|SCHAR|WCHAR|SHORT|SMALLINT|INT|INTEGER|LONG|BIGINT|REAL|FLOAT|DOUBLE|DATE|TIME|TIMESTAMP|DATETIME)\\s*(.*)\\s*$";

	/** 检查列标记. 后缀三种情况:空格|逗号和其它参数|空格和其它参数 **/
	private final static String COLUMN_CHECKTAG = "^\\,\\s*(\\w+)\\s+(?i)(RAW|BINARY|CHAR|SCHAR|WCHAR|SHORT|SMALLINT|INT|INTEGER|LONG|BIGINT|FLOAT|REAL|DOUBLE|DATE|TIME|DATETIME|TIMESTAMP)(\\s*|\\s*\\,.+|\\s+.+)$";
	
	/** 删除表。格式: DROP TABLE schema.table **/
	private final static String DROP_TABLE = "^\\s*(?i)(?:DROP\\s+TABLE)\\s+(\\w+)\\.(\\w+)\\s*$";
	/** 显示表属性。格式: SHOW TABLE schema.table **/
	private final static String SHOW_TABLE = "^\\s*(?i)(?:SHOW\\s+TABLE)\\s+(\\w+)\\.(\\w+)\\s*$";

	/**
	 * default
	 */
	public TableParser() {
		super();
	}
	
	/**
	 * 解析 "SM|STORAGEMODEL=DSM|NSM|ROWS|COLUMNS"
	 * 数据在主机上的物理存储模式，分为行存储(NSM)和列存储(DSM)
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	private String splitStorageModel(Table table, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_PREFIX_STORAGEMODEL);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return sql; // 不匹配,原样返回
		}
		
		String model = matcher.group(1);
		sql = matcher.group(2);
		if ("NSM".equalsIgnoreCase(model) || "ROWS".equalsIgnoreCase(model)) {
			table.setStorage(Type.NSM);
		} else if ("DSM".equalsIgnoreCase(model) || "COLUMNS".equalsIgnoreCase(model)) {
			table.setStorage(Type.DSM);
		}
		// 返回剩余字符串
		return matcher.group(2);
	}

	/**
	 * 解析 "hostmodel=share|exclusive"
	 * 被分配表在主机上的存在状态: 共亨或者独占
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	private String splitHostModel(Table table, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_PREFIX_HOSTMODE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return sql; // 不匹配,原样返回
		}

		// 匹配,取出数据
		String model = matcher.group(1);
		if ("share".equalsIgnoreCase(model)) {
			table.setMode(Table.SHARE);
		} else if ("exclusive".equalsIgnoreCase(model)) {
			table.setMode(Table.EXCLUSIVE);
		}
		// 返回剩余字符串
		return matcher.group(2);
	}
	
	/**
	 * 解析 "hostcache=[yes|no]"
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	private String splitHostCache(Table table, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_PREFIX_HOSTCACHE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return sql;
		}

		// 匹配,判断结果
		String mode = matcher.group(1);
		table.setCaching("YES".equalsIgnoreCase(mode));
		// 返回剩余字节
		return matcher.group(2);
	}
	
	/**
	 * 解析 "chunksize={digit}M"
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	private String splitChunkSize(Table table, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_PREFIX_CHUNKSIZE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return sql; // 不匹配,原样返回
		}

		// 匹配,计算数据块尺寸
		String size = matcher.group(1);
		int value = java.lang.Integer.parseInt(size);
		table.setChunkSize(value * SQLParser.m);
		// 返回剩余字符串
		return matcher.group(2);
	}
	
	/**
	 * 解析 "chunkcopy=[digit]"
	 * @param table
	 * @param sql
	 * @return
	 */
	private String splitChunkCopy(Table table, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_PREFIX_CHUNKCOPY);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return sql; // 不匹配,原值返回
		}

		// 匹配,数据的备份数(一个主块,N个从块)
		String num = matcher.group(1);
		table.setCopy(java.lang.Integer.parseInt(num));
		// 返回剩余字节
		return matcher.group(2);
	}
	
	/**
	 * 解析 "primehosts={digit}"
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	private String splitPrimeHosts(Table table, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_PREFIX_PRIMEHOSTS);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return sql;
		}

		String hosts = matcher.group(1);
		table.setPrimes(java.lang.Integer.parseInt(hosts));
		return matcher.group(2);
	}
	
	/**
	 * 解析 "clusters=[digit]" "clusters='[ip|host]'"
	 * 分配HOME节点集群的数量 或者 指定HOME节点主机地址
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	private String splitCluster(Table table, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_PREFIX_CLUSTERS_NUMBER);
		Matcher matcher = pattern.matcher(sql);

		// 解析主机数量
		if (matcher.matches()) {
			String size = matcher.group(1);
			Cluster cluster = table.getCluster();
			cluster.setSites(java.lang.Integer.parseInt(size));
			// 返回剩余字节
			return matcher.group(2);
		}

		// 解析HOME主机地址
		pattern = Pattern.compile(TableParser.TABLE_PREFIX_CLUSTERS_ADDRESS);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String groups = matcher.group(1);
			Cluster cluster = table.getCluster();
			// 以逗号为分隔符,解析主机地址(IP和域名)
			String[] items = groups.split(",");
			for (String item : items) {
				// 解析地址，如果是匹配就保存
				try {
					Address address = new Address(item);
					cluster.add(address);
				} catch (UnknownHostException e) {
					throw new SQLSyntaxException(e);
				}
			}
			// 返回剩余字节
			return matcher.group(2);
		}

		return sql;
	}
	
	/**
	 * 分析在"CREATE TABLE" 和 "SCHEMA-NAME.TABLE-NAME"之间的数据
	 * 
	 * @param table
	 * @param prefix
	 */
	private void splitTablePrefix(Table table, String prefix) {
		while (prefix.trim().length() > 0) {
			// 1. 数据存储模式
			String result = this.splitStorageModel(table, prefix);
			if (!prefix.equals(result)) {
				prefix = result;
				continue;
			}
			// 2. 表在主机的存在模式(共亨物理空间还是独占)
			result = this.splitHostModel(table, prefix);
			if (!prefix.equals(result)) {
				prefix = result;
				continue;
			}
			// 3. 主机存缓模式
			result = this.splitHostCache(table, prefix);
			if (!prefix.equals(result)) {
				prefix = result;
				continue;
			}
			// 4. 数据块尺寸
			result = this.splitChunkSize(table, prefix);
			if (!prefix.equals(result)) {
				prefix = result;
				continue;
			}
			// 5. 数据块备份数(一个主块,N个从块)
			result = splitChunkCopy(table, prefix);
			if (!prefix.equals(result)) {
				prefix = result;
				continue;
			}
			// 6. 表分配到主节点(DATA NODE)数量
			result = splitPrimeHosts(table, prefix);
			if (!prefix.equals(result)) {
				prefix = result;
				continue;
			}
			// 7. 指定本次表被分配多少个HOME节点
			result = splitCluster(table, prefix);
			if (!prefix.equals(result)) {
				prefix = result;
				continue;
			}
			// 8. 出错
			throw new SQLSyntaxException("syntax error:%s", prefix);
		}
	}


	/**
	 * 解析 列属性中的 "NOT NULL|NULL"
	 * 
	 * @param attribute
	 * @param sql
	 * @return
	 */
	private String splitNull(ColumnAttribute attribute, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_COLUMN_NULLORNOT);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String s = matcher.group(1);
			attribute.setNull("NULL".equalsIgnoreCase(s)); 	//允许空 或者不允许
			return matcher.group(2); 	//返回剩下的字符串
		}
		// 不匹配,完全返回
		return sql;
	}
	
	/**
	 * 解析列属性中的 "NOT CASE|CASE"
	 * 
	 * @param attribute
	 * @param sql
	 * @return
	 */
	private String splitCase(ColumnAttribute attribute, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_COLUMN_CASEORNOT);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			if(!attribute.isWord()) {
				throw new SQLSyntaxException("%s cannot support 'CASE or NOT CASE'", attribute.getName());
			}
			
			WordAttribute instan = (WordAttribute)attribute;
			String s = matcher.group(1);
			instan.setSentient("CASE".equalsIgnoreCase(s)); //CASE，大小写敏感。NOT CASE，大小写不敏感
			
			return matcher.group(2); 	// 返回剩下的字符串
		}
		// 不匹配,完全返回
		return sql;
	}

	/**
	 * 解析列属性中的 "NOT LIKE|LIKE"
	 * 
	 * @param attribute
	 * @param sql
	 * @return
	 */
	private String splitLike(ColumnAttribute attribute, String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_COLUMN_LIKEORNOT);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			if (!attribute.isWord()) {
				throw new SQLSyntaxException("%s cannot support 'LIKE or NOT LIE'", attribute.getName());
			}

			WordAttribute instan = (WordAttribute) attribute;
			String s = matcher.group(1);
			instan.setLike("LIKE".equalsIgnoreCase(s));

			return matcher.group(2); // 返回剩下的字符串
		}
		// 不匹配,完全返回
		return sql;
	}
	
	/**
	 * 解析 "DEFAULT [...]"
	 * 
	 * @param attribute
	 * @param sql
	 * @return
	 */
	private String splitDefault(ColumnAttribute attribute, String sql) {
		Pattern pattern = Pattern.compile(TableParser.DEFAULT_VARIABLE);
		Matcher matcher = pattern.matcher(sql);

		//1. 字符串格式判断。包括字符和日期
		if(matcher.matches()) {
			String s = matcher.group(1);
			if (attribute.isWord()) {
				// 字符需要编码保存
				WordAttribute instan = (WordAttribute) attribute;
				Charset charset = VariableGenerator.getCharset(attribute);
				instan.setValue( charset.encode(s) );
			} else if (attribute.isDate()) {
				// 转换为日期格式
				int date = super.splitDate(s);
				((DateAttribute) attribute).setValue(date);
			} else if (attribute.isTime()) {
				// 转换为时间格式
				int time = super.splitTime(s);
				((TimeAttribute) attribute).setValue(time);
			} else if (attribute.isTimestamp()) {
				// 转换为时间戳格式
				long timestamp = super.splitTimestamp(s);
				((TimestampAttribute) attribute).setValue(timestamp);
			} else {
				throw new SQLSyntaxException("%s cannot support %s", attribute.getName(), sql);
			}
			return matcher.group(2); // 返回剩下的字符串
		}
		
		//2. 浮点数格式判断
		pattern = Pattern.compile(TableParser.DEFAULT_DIGIT_FLOAT);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String s = matcher.group(1);
			if(attribute.isFloat()) {
				float value = java.lang.Float.parseFloat(s);
				((FloatAttribute) attribute).setValue(value);
			} else if(attribute.isDouble()) {
				double value = java.lang.Double.parseDouble(s);
				((DoubleAttribute) attribute).setValue(value);
			} else {
				throw new SQLSyntaxException("%s is invalid!", sql);
			}
			return matcher.group(2);
		}

		//3. 整数格式判断
		pattern = Pattern.compile(TableParser.DEFAULT_DIGIT_CONST);
		matcher = pattern.matcher(sql);
		if ( matcher.matches()) {
			String s = matcher.group(1);
			if (attribute.isShort()) {
				short value = java.lang.Short.parseShort(s);
				((ShortAttribute) attribute).setValue(value);
			} else if(attribute.isInteger()) {
				int value = java.lang.Integer.parseInt(s);
				((IntegerAttribute) attribute).setValue(value);
			} else if(attribute.isLong()) {
				long value = java.lang.Long.parseLong(s);
				((LongAttribute) attribute).setValue(value);
			} else {
				throw new SQLSyntaxException("%s is invalid!", sql);
			}
			return matcher.group(2);
		}
		
		//4. 函数格式判断
		pattern = Pattern.compile(TableParser.DEFAULT_FUNCTION);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String description = matcher.group(1);
			SQLFunction function = SQLFunctionCreator.create(null, description);
			if(function == null) {
				throw new SQLSyntaxException("invalid function: %s", sql);
			}			
			attribute.setFunction(function);

			return matcher.group(2);
		}
		
		// 不匹配,完全返回
		return sql;

	}

	/**
	 * 解析"PACKING [...]"子句
	 * 
	 * @param attribute
	 * @param sql
	 */
	private String splitPacking(ColumnAttribute attribute, String sql) {
		String encrypt = null;
		byte[] password = null;
		String compress = null;
		String suffix = null;
		
		Pattern pattern = Pattern.compile(TableParser.ENCRYPT_COMPRESS);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if(match) {
			encrypt = matcher.group(1);
			password = matcher.group(2).getBytes();
			compress = matcher.group(3);
			suffix = matcher.group(4);
		}
		if(!match) {
			pattern = Pattern.compile(TableParser.COMPRESS_ENCRYPT);
			matcher = pattern.matcher(sql);
			if(match = matcher.matches()) {
				compress = matcher.group(1);
				encrypt = matcher.group(2);
				password = matcher.group(3).getBytes();
				suffix = matcher.group(4);
			}
		}
		if(!match) {
			pattern = Pattern.compile(TableParser.ENCRYPT);
			matcher = pattern.matcher(sql);
			if(match = matcher.matches()) {
				encrypt = matcher.group(1);
				password = matcher.group(2).getBytes();
				suffix = matcher.group(3);
			}
		}
		if(!match) {
			pattern = Pattern.compile(TableParser.COMPRESS);
			matcher = pattern.matcher(sql);
			if(match = matcher.matches()) {
				compress = matcher.group(1);
				suffix = matcher.group(2);
			}
		}
		
		// 如果最后不匹配,原值返回
		if(!match) return sql; 

		// PACKING 属性只限可变长类型
		if (!attribute.isVariable()) {
			throw new SQLSyntaxException("%s is not variable", attribute.getName());
		}
		
		VariableAttribute variable = (VariableAttribute)attribute;
		
		int encryptId = 0, compressId = 0;
		
		// 压缩算法名称
		if ("GZIP".equalsIgnoreCase(compress)) {
			compressId = Packing.GZIP;
		} else if ("ZIP".equalsIgnoreCase(compress)) {
			compressId = Packing.ZIP;
		}
		// 加密算法名称
		if ("DES".equalsIgnoreCase(encrypt)) {
			encryptId = Packing.DES;
		} else if ("DES3".equalsIgnoreCase(encrypt) || "3DES".equalsIgnoreCase(encrypt)) {
			encryptId = Packing.DES3;
		} else if ("AES".equalsIgnoreCase(encrypt)) {
			encryptId = Packing.AES;
		} else if ("blowfish".equalsIgnoreCase(encrypt)) {
			encryptId = Packing.BLOWFISH;
		}
		
		variable.setPacking(compressId, encryptId, password);

		return suffix;
	}
	
	/**
	 * 解析主键和从键.可变长类型可指定字符长度(注意，是字符，不是字节)
	 * 格式: "PRIME KEY|SLAVE KEY [(key size)]"
	 * 
	 * @param attribute
	 * @param sql
	 * @return
	 */
	private String splitKey(ColumnAttribute attribute, String sql) {
		String key = null;
		int indexLimit = 0;
		String suffix = null;

		Pattern pattern = Pattern.compile(TableParser.INDEXKEY_LIMIT);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (match) {
			key = matcher.group(1);
			indexLimit = java.lang.Integer.parseInt(matcher.group(2));
			suffix = matcher.group(3);
		}
		if (!match) {
			pattern = Pattern.compile(TableParser.INDEXKEY);
			matcher = pattern.matcher(sql);
			if (match = matcher.matches()) {
				key = matcher.group(1);
				suffix = matcher.group(2);
			}
		}

		// 不匹配，原值返回
		if (!match) return sql;
		
		// 判断是主键还是从键
		byte rank = (key.matches("^(?i)(PRIME\\s+KEY)$") ? Type.PRIME_KEY : Type.SLAVE_KEY);
		attribute.setKey(rank);
		
		// 如果定义截取字节长度，必须是可变长类型
		if (indexLimit > 0) {
			if (!attribute.isVariable()) { // 必须有可变长类型
				throw new SQLSyntaxException("'%s' is not vairable!", attribute.getName());
			}
			((VariableAttribute) attribute).setIndexSize(indexLimit);
		}

		// 返回剩余字符
		return suffix;
	}

	/**
	 * 解析列和它的相关属性
	 * 
	 * @param sql
	 * @return
	 */
	private ColumnAttribute splitColumnAttribute(String sql) {
		Pattern pattern = Pattern.compile(TableParser.TABLE_COLUMN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid syntax: %s", sql);
		}

		String name = matcher.group(1);
		String type = matcher.group(2);
		String suffix = matcher.group(3);
		
		ColumnAttribute attribute = ColumnAttributeCreator.create(type);
		if(attribute == null) {
			throw new SQLSyntaxException("invalid column attribute: %s", sql);
		}
		attribute.setName(name);
		
		// 解析列类型之外的其它属性
		while(suffix.trim().length() > 0) {
			//1. 空值判断
			String result = this.splitNull(attribute, suffix);
			if (!suffix.equals(result)) {
				suffix = result; continue;
			}
			//2. 大小写开关判断
			result = this.splitCase(attribute, suffix);
			if (!suffix.equals(result)) {
				suffix = result; continue;
			}
			//3. 模糊检索判断
			result = this.splitLike(attribute, suffix);
			if (!suffix.equals(result)) {
				suffix = result; continue;
			}
			//4. 索引键判断
			result = this.splitKey(attribute, suffix);
			if (!suffix.equals(result)) {
				suffix = result; continue;
			}
			//5. 默认值判断
			result = this.splitDefault(attribute, suffix);
			if (!suffix.equals(result)) {
				suffix = result; continue;
			}
			//6. 打包判断
			result = this.splitPacking(attribute, suffix);
			if (!suffix.equals(result)) {
				suffix = result; continue;
			}
			
			// 全部判断结束，仍未找到匹配的结果，是错误
			throw new SQLSyntaxException("cannot resolve %s", suffix);
		}		
		return attribute;
	}

	/**
	 * 区分列属性之间关系的标记符是逗号，以逗号加下一列名和列类型，可以实现分割列属性
	 * 
	 * @param sql
	 * @return
	 */
	private String[] splitTableColumns(String sql) {
		int index = 0, seek = 0;
		ArrayList<String> array = new ArrayList<String>();
		while (seek < sql.length()) {
			char w = sql.charAt(seek++);
			if (w != ',') {
				continue;
			}
			String suffix = sql.substring(seek - 1);
			if (!suffix.matches(TableParser.COLUMN_CHECKTAG)){
				continue;
			}
			String prefix = sql.substring(index, seek - 1);
			if (prefix.trim().length() > 0) {
				array.add(prefix);
				index = seek; // 跨过逗号
			}
		}
		
		if (index < sql.length()) {
			String suffix = sql.substring(index);
			if (suffix.trim().length() > 0) array.add(suffix);
		}

//		// debug code, start
//		System.out.println(sql);
//		for(String item : array) {
//			System.out.printf("%s\n", item);
//		}
//		// debug code, end

		String[] s = new String[array.size()];
		return array.toArray(s);
	}

	/**
	 * 根据表中参数定义，生成Table实例
	 * 
	 * @param sql
	 * @param sqlIndex
	 * @return
	 */
	private Table splitTable(String sql) {
		String prefix = new String();
		String schemaName = new String();
		String tableName = new String();
		String suffix = new String();
		
		// 拆分"Create Table"中的属性
		Pattern pattern = Pattern.compile(TableParser.CREATE_TABLE1);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (match) {
			prefix = matcher.group(1);
			schemaName = matcher.group(2);
			tableName = matcher.group(3);
			suffix = matcher.group(4);
		}
		if (!match) {
			pattern = Pattern.compile(TableParser.CREATE_TABLE2);
			matcher = pattern.matcher(sql);
			if (match = matcher.matches()) {
				schemaName = matcher.group(1);
				tableName = matcher.group(2);
				suffix = matcher.group(3);
			}
		}
		if(!match) {
			throw new SQLSyntaxException("illegal table syntax!");
		}

		// 数据库和表的长度判断，不能超过64字节
		if (!Space.isSchemaSize(schemaName.length())) {
			throw new SQLSyntaxException("database sizeout! >64!");
		} else if (!Space.isTableSize(tableName.length())) {
			throw new SQLSyntaxException("table sizeout! >64!");
		}

		// 建表
		Space space = new Space(schemaName, tableName);
		Table table = new Table(space);
		// 解析针对表的系统参数
		splitTablePrefix(table, prefix);
		// 将表的列属性集合分成多个独立单元
		String[] items = splitTableColumns(suffix);
		if (items == null || items.length == 0) {
			throw new SQLSyntaxException("invalid sql table: %s!", sql);
		}
		
		// 分析各列的参数定义，列ID从1开始
		short columnId = 1;
		for (String item : items) {
			ColumnAttribute attribute = splitColumnAttribute(item);
			if (attribute == null) {
				throw new SQLSyntaxException("illegal column attribute: %s", item);
			}
			if (table.find(attribute.getName()) != null) {
				throw new SQLSyntaxException("duplicate column: %s", attribute.getName());
			}
			attribute.setColumnId(columnId++);
			table.add(attribute);
		}

		// 检查主键和从键的数量(主键只能有一个，从键允许任意多个)
		int prime = 0, slave = 0;
		for (ColumnAttribute attribute : table.values()) {
			if (attribute.isPrimeKey()) prime++;
			else if (attribute.isSlaveKey()) slave++;
		}
		if (prime != 1) {
			throw new SQLSyntaxException("prime key size is %d", prime);
		}
		if (prime == 0 && slave == 0) {
			throw new SQLSyntaxException("cannot set key");
		}
		return table;
	}

	/**
	 * 解析"Create Table...", 生成Table对象
	 * 
	 * @param sql
	 * @param sqlIndex
	 * @return
	 */
	public Table splitCreateTable(String sql, boolean online, SQLChooser chooser) {
		// 解析表中所有参数
		Table table = splitTable(sql);
		
		// 检查数据库
		if (online && !chooser.onSchema(table.getSpace().getSchema())) {
			throwable("cannot find %s", table.getSpace().getSchema());
		}
		// 检查数据库表
		if (online && chooser.onTable(table.getSpace())) {
			throwable("%s existed!", table.getSpace());
		}

		// 如果是列存储，所有列都是索引
		if (table.isDSM()) {
			for (ColumnAttribute attribute : table.values()) {
				if (attribute.isNoneKey()) attribute.setKey(Type.SLAVE_KEY);
			}
		}
		
		// 检查参数
		
		// 确定分配的HOME集群数量 (在几个HOME集群上建表)
		Cluster clusters = table.getCluster();
		if (clusters.getSites() == 0 && clusters.isEmpty()) {
			clusters.setSites(1);
		}
		
		// 重新定义可变长类型的值和索引
		for (ColumnAttribute attribute : table.values()) {
			// 必须是可变长，且是索引类型时，才允许继续			
			if (!(attribute.isVariable() && attribute.isKey())) continue;

			VariableAttribute variable = (VariableAttribute) attribute;
			// 取出数据，如果是字符类型，已经编码，使用前需要解码
			byte[] origin = variable.getValue();
			if (origin == null) continue;

			// 根据打包算法进行重新编码
			try {
				if (attribute.isWord()) {
					WordAttribute word = (WordAttribute) attribute;
					Charset charset = VariableGenerator.getCharset(word);
					// 字符串解码
					String text = charset.decode(origin, 0, origin.length);

					// 设置经过编码和打包(压缩，加密)的字符值勤
					byte[] value = VariableGenerator.toValue(word, text);
					word.setValue(value);
					// 设置索引(可能为NULL)
					byte[] index = VariableGenerator.toIndex(table.isDSM(), word, text);
					word.setIndex(index);				
					// 设置LIKE相关参数
					java.util.List<VWord> set = VariableGenerator.toVWord(word, text);
					if (set != null) word.addVWords(set);
				} else {
					byte[] result = VariableGenerator.toValue(variable, origin, 0, origin.length);
					variable.setValue(result);

					byte[] index = VariableGenerator.toIndex(table.isDSM(), variable, origin);
					variable.setIndex(index);
				}
			} catch (IOException e) {
				throw new SQLSyntaxException(e);
			}
		}
		
		return table;
	}

	/**
	 * 取出被删除的表名
	 * 
	 * @param sql
	 * @return
	 */
	public Space splitDropTable(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(TableParser.DROP_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error os missing!");
		}
		
		Space space = new Space(matcher.group(1), matcher.group(2));
		if (online && !chooser.onTable(space)) {
			throwable("cannot find %s", space);
		}
		return space;
	}
	
	/**
	 * 取出需要显示的表名
	 * 
	 * @param sql
	 * @return
	 */
	public Space splitShowTable(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(TableParser.SHOW_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error os missing!");
		}
		
		Space space = new Space(matcher.group(1), matcher.group(2));
		if (online && !chooser.onTable(space)) {
			throwable("cannot find %s", space);
		}
		return space;
	}
}