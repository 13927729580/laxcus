/**
 *
 */
package com.lexst.sql.parse;

import java.util.regex.*;

import com.lexst.sql.account.*;
import com.lexst.sql.parse.result.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;

/**
 * SQL命令检查器，判断用户输入的SQL是否合法。<br>
 *
 */
public class SQLChecker {
	/** 用户账号建立、删除、修改 */
	private final static String CREATE_USER = "^\\s*(?i)(?:CREATE\\s+USER)(\\s+.+|\\s*)$";
	private final static String DROP_USER = "^\\s*(?i)(?:DROP\\s+USER)(\\s+.+|\\s*)$";
	private final static String DROP_SHA1USER = "^\\s*(?i)(?:DROP\\s+SHA1\\s+USER)(\\s+.+|\\s*)$";
	private final static String ALTER_USER = "^\\s*(?i)(?:ALTER\\s+USER)(\\s+.+|\\s*)$";

	/** 权限授予、回收 **/
	private final static String GRANT  = "^\\s*(?i)(?:GRANT)(\\s+.+|\\s*)$";
	private final static String REVOKE = "^\\s*(?i)(?:REVOKE)(\\s+.+|\\s*)$";
	
	/** 数据库建立、删除、显示 **/
	private final static String CREATE_SCHEMA = "^\\s*(?i)(?:CREATE\\s+SCHEMA|CREATE\\s+DATABASE)(\\s+.+|\\s*)$";
	private final static String DROP_SCHEMA = "^\\s*(?i)(?:DROP\\s+SCHEMA|DROP\\s+DATABASE)(\\s+.+|\\s*)$";
	private final static String SHOW_SCHEMA = "^\\s*(?i)(?:SHOW\\s+SCHEMA|SHOW\\s+DATABASE)\\s+(\\w+)\\s*$";

	/** 数据库表建立、删除、显示 */
	private final static String CREATE_TABLE = "^\\s*(?i)(?:CREATE\\s+TABLE)(\\s+.+|\\s*)$";
	private final static String DROP_TABLE =   "^\\s*(?i)(?:DROP\\s+TABLE)(\\s+.+|\\s*)$";
	private final static String SHOW_TABLE =   "^\\s*(?i)(?:SHOW\\s+TABLE)(\\s+.+|\\s*)$";

	/** 记录写入 **/
	private final static String INSERT_PATTERN = "^\\s*(?i)(?:INSERT\\s+INTO)(\\s+.+|\\s*)$";
	private final static String INJECT_PATTERN = "^\\s*(?i)(?:INJECT\\s+INTO)(\\s+.+|\\s*)$";
	
	/** 记录检索、删除、更新 **/
	private final static String SELECT_PATTERN = "^\\s*(?i)(?:SELECT)(\\s+.+|\\s*)$";
	private final static String DELETE_PATTERN = "^\\s*(?i)(?:DELETE\\s+FROM)(\\s+.+|\\s*)$";
	private final static String UPDATE_PATTERN = "^\\s*(?i)(?:UPDATE)(.+?)(?i)(?:SET)(\\s+.+|\\s*)$";
	
	/** 数据重构(删除冗余数据和优化排序)**/
	private final static String REBUILD_PATTERN = "^\\s*(?i)(?:REBUILD)(\\s+.+|\\s*)$";

	/** 索引加载、卸载 **/
	private final static String LOAD_INDEX = "^\\s*(?i)(?:LOAD\\s+INDEX)(\\s+.+|\\s*)$";
	private final static String STOP_INDEX = "^\\s*(?i)(?:STOP\\s+INDEX|UNLOAD\\s+INDEX)(\\s+.+|\\s*)$";
	
	/** 数据块加载、卸载 **/
	private final static String LOAD_CHUNK = "^\\s*(?i)(?:LOAD\\s+CHUNK)(\\s+.+|\\s*)$"; 
	private final static String STOP_CHUNK = "^\\s*(?i)(?:STOP\\s+CHUNK|UNLOAD\\s+CHUNK)(\\s+.+|\\s*)$"; 
		
	/** 数据块尺寸设置、显示 **/
	private final static String SET_CHUNKSIZE = "^\\s*(?i)(?:SET\\s+CHUNKSIZE)(\\s+.+|\\s*)$";
	private final static String SHOW_CHUNKSIZE ="^\\s*(?i)(?:SHOW\\s+CHUNKSIZE)(\\s+.+|\\s*)$";
	
	/** 根据命名任务重构数据(此命令作用于BUILD节点) **/
	private final static String BUILD_TASK = "^\\s*(?i)(?:BUILD\\s+TASK)\\s+(\\w+)(\\s+(?i)TO\\s+.+|\\s*)$";

	/** 分布计算语句conduct **/
	private final static String CONDUCT_PATTERN = "^\\s*(?i)CONDUCT\\s+(.*)(?i)(?:FROM\\s+NAMING\\s*\\:)(.+)(?i)(?:TO\\s+NAMING\\s*\\:)(.+)$";
	
	/** 设置数据重构时间 */
	private final static String SET_SWITCHTIME = "^\\s*(?i)(?:SET\\s+SWITCH\\s+TIME)(\\s+.+|\\s*)$";
	
	/** 打印当前HOME集群下的全部或者某类节点的IP地址 */
	private final static String SHOW_SITE = "^\\s*(?i)(?:SHOW\\s+SITE)\\s+(?i)(ALL|HOME|LOG|DATA|WORK|BUILD|CALL)(\\s+(?i)FROM\\s+.+|\\s*)$"; 
	
	/** 设置数据存储目录(在SQLive、SQLive console) */
	private final static String SET_COLLECT_PATH = "^\\s*(?i)(?:SET\\s+COLLECT\\s+PATH)\\s+(.+)\\s*$";
	
	/** 测试分布计算的回收命名对象  */
	private final static String TEST_COLLECT_TASK = "^\\s*(?i)(?:TEST\\s+COLLECT\\s+TASK)\\s+(.+)\\s*$";
	
	private final static String SHOW_TASK = "^\\s*(?i)(?:SHOW\\s+TASK)\\s+(.+)\\s*$";

	/**
	 * default
	 */
	public SQLChecker() {
		super();
	}
	
	/**
	 * 检查"SET COLLECT PATH"语句
	 * @param sql
	 * @return
	 */
	public boolean isSetCollectPath(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SET_COLLECT_PATH);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	/**
	 * 测试"TEST COLLECT TASK"语句
	 * @param sql
	 * @return
	 */
	public boolean isTestCollectPath(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.TEST_COLLECT_TASK);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	/**
	 * 检查是否匹配"SHOW TASK ..."语句
	 * @param sql
	 * @return
	 */
	public boolean isShowTask(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_TASK);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	/**
	 * 检查是否匹配"SHOW SITE"语句
	 * @param sql - 语法格式: SHOW SITE [ALL|HOME|LOG|DATA|CALL|BUILD|WORK]
	 * @return
	 */
	public boolean isShowSite(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_SITE);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	/**
	 * 检查是否匹配"CREATE SCHEMA|CREATE DATABASE"语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isCreateSchema(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.CREATE_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		SchemaParser parser = new SchemaParser();
		return parser.splitCreateSchema(sql, false, chooser) != null;
	}
	
	/**
	 * 检查是否匹配"DROP SCHEMA|DROP DATABASE"语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isDropSchema(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.DROP_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		SchemaParser parser = new SchemaParser();
		return parser.splitDropSchema(sql, false, chooser) != null;
	}

	/**
	 * 检查是否匹配"SHOW SCHEMA|SHOW DATABASE"语句
	 * @param sql
	 * @return
	 */
	public boolean isShowSchema(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		SchemaParser parser = new SchemaParser();
		return parser.splitShowSchema(sql, false, chooser) != null;
	}
	
	/**
	 * 检查"CREATE USER"语句
	 * @param sql
	 * @return
	 */
	public boolean isCreateUser(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.CREATE_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		UserParser parser = new UserParser();
		User user = parser.splitCreateUser(sql, false, chooser);
		return user != null;
	}

	/**
	 * 检查"DROP USER"语句
	 * @param sql
	 * @return
	 */
	public boolean isDropUser(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.DROP_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		UserParser parser = new UserParser();
		User user = parser.splitDropUser(sql, false, chooser);
		return user != null;
	}
	
	/**
	 * 检查"DROP SHA1 USER"语句
	 * @param sql
	 * @return
	 */
	public boolean isDropSHA1User(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DROP_SHA1USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		UserParser parser = new UserParser();
		User user = parser.splitDropSHA1User(sql);
		return user != null;
	}

	/**
	 * 检查"ALTER USER"语句
	 * @param sql
	 * @return
	 */
	public boolean isAlterUser(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.ALTER_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		UserParser parser = new UserParser();
		User user = parser.splitAlterUser(sql, false, chooser);
		return user != null;
	}

	/**
	 * 检查权限设置 "GRANT ..."语句
	 * @param sql
	 * @return
	 */
	public boolean isGrant(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.GRANT);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		GrantParser parser = new GrantParser();
		Permit permit = parser.split(sql, false, chooser);
		return permit != null;
	}

	/**
	 * 检查回收权限"REVOKE ..."语句
	 * @param sql
	 * @return
	 */
	public boolean isRevoke(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.REVOKE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		RevokeParser parser = new RevokeParser();
		Permit permit = parser.split(sql, false, chooser);
		return permit != null;
	}

	/**
	 * 检查"CREATE TABLE ..."语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isCreateTable(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.CREATE_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		TableParser parser = new TableParser();
		Table table = parser.splitCreateTable(sql, false, chooser);
		return table != null;
	}
	
	/**
	 * 检查"DROP TABLE..."语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isDropTable(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.DROP_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		TableParser parser = new TableParser();
		Space space = parser.splitDropTable(sql, false, chooser);
		return space != null;
	}

	/**
	 * 检查"SHOW TABLE ..."语句
	 * @param sql
	 * @return
	 */
	public boolean isShowTable(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		TableParser parser = new TableParser();
		Space space = parser.splitShowTable(sql, false, chooser);
		return space != null;
	}

//	/**
//	 * 从SQL SELECT语句中提取数据表名称
//	 * 
//	 * @param sql
//	 * @return
//	 */
//	public Space getSelectSpace(String sql) {
//		Pattern pattern = Pattern.compile(SQLChecker.SELECT_SPACE);
//		Matcher matcher = pattern.matcher(sql);
//		if (!matcher.matches()) {
//			return null;
//		}
//		String schema = matcher.group(2);
//		String table = matcher.group(3);
//		return new Space(schema, table);
//	}

	/**
	 * 测试SQL SELECT语句
	 * 
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isSelect(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.SELECT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		SelectParser parser = new SelectParser();
		Select select = parser.split(sql, chooser);
		return select != null;
	}

	/**
	 * 测试"DELETE FROM"语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isDelete(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.DELETE_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		DeleteParser parser = new DeleteParser();
		Delete delete = parser.split(sql, chooser);
		return delete != null;
	}

	/**
	 * 测试"INSERT INTO"语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isInsert(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.INSERT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		InsertParser parser = new InsertParser();
		Insert insert = parser.splitInsert(sql, chooser);
		return insert != null;
	}
	
	/**
	 * 测试"INJECT INTO"语句
	 * 
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isInject(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.INJECT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		InsertParser parser = new InsertParser();
		Inject inject = parser.splitInject(sql, chooser);
		return inject != null;
	}
	
	/**
	 * 测试"UPDATE ... SET"语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isUpdate(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.UPDATE_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		UpdateParser parser = new UpdateParser();
		Update update = parser.split(sql, chooser);
		return update != null;
	}
	
	/**
	 * 测试 "CONDUCT ..."语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isConduct(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.CONDUCT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		ConductParser parser = new ConductParser();
		Conduct conduct = parser.split(sql, chooser);
		return conduct != null;
	}
	
	/**
	 * 判断并且解析数据块尺寸
	 * 
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public boolean isSetChunkSize(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.SET_CHUNKSIZE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		ChunkParser parser = new ChunkParser();
		Object[] objects = parser.splitSetChunkSize(sql, false, chooser);
		return objects != null;
	}

	/**
	 * 检查"SHOW CHUNKSIZE ..."语句
	 * @param sql
	 * @return
	 */
	public boolean isShowChunkSize(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_CHUNKSIZE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		ChunkParser parser = new ChunkParser();
		ChunkHostResult host = parser.splitShowChunkSize(sql, false, chooser);
		return host != null;
	}

	/**
	 * 检测数据重组时间语句
	 * 
	 * @param sql - 语法格式: set switch time schema.table (hourly|daily|weekly|monthly) 'time style' [order by column-name]
	 * @return
	 */
	public boolean isSwitchTime(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.SET_SWITCHTIME);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		SwitchTimeParser parser = new SwitchTimeParser();
		return parser.split(sql, chooser) != null;
	}

	/**
	 * 检测数据重构语句
	 * 
	 * @param sql - 语法格式: rebuild schema.table [order by [column-name]] [to [ip address...]]
	 * @param chooser
	 * @return
	 */
	public boolean isRebuild(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.REBUILD_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		RebuildParser parser = new RebuildParser();
		return parser.split(sql, chooser) != null;
	}

	/**
	 * 检查是否匹配"LOAD INDEX"语句
	 * @param sql - 语法格式: LOAD INDEX schema.table [TO address, address, ...]
	 * @return
	 */
	public boolean isLoadIndex(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.LOAD_INDEX);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		IndexParser parser = new IndexParser();
		return parser.splitLoadIndex(sql, false, chooser) != null;
	}
	
	/**
	 * 检查是否匹配"STOP INDEX"语句
	 * @param sql - 语法格式: STOP INDEX|UNLOAD INDEX schema.table [FROM address, address, ...]
	 * @return
	 */
	public boolean isStopIndex(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.STOP_INDEX);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		IndexParser parser = new IndexParser();
		return parser.splitStopIndex(sql, false, chooser) != null;
	}
	
	/**
	 * 检查是否匹配"LOAD CHUNK"语句
	 * @param sql - 语法格式: LOAD CHUNK schema.table [TO address, address, ...]
	 * @return
	 */
	public boolean isLoadChunk(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.LOAD_CHUNK);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		ChunkParser parser = new ChunkParser();
		return parser.splitLoadChunk(sql, false, chooser) != null;
	}

	/**
	 * 检查是否匹配"STOP CHUNK|UNLOAD CHUNK"语句
	 * @param sql - 语法格式: STOP CHUNK|UNLOAD CHUNK schema.table [FROM address, address, ...]
	 * @return
	 */
	public boolean isStopChunk(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SQLChecker.STOP_CHUNK);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		ChunkParser parser = new ChunkParser();
		return parser.splitStopChunk(sql, false, chooser) != null;
	}
	
	/**
	 * 检查启动BUILD任务的语法是否正常
	 * @param sql
	 * @return
	 */
	public boolean isBuildTask(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.BUILD_TASK);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

}