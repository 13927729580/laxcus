/**
 * 
 */
package com.lexst.live.console;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import com.lexst.algorithm.collect.*;
import com.lexst.fixp.*;
import com.lexst.live.*;
import com.lexst.live.pool.*;
import com.lexst.log.client.*;
import com.lexst.site.live.*;
import com.lexst.sql.*;
import com.lexst.sql.account.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.parse.*;
import com.lexst.sql.parse.result.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.util.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;
import com.lexst.visit.*;

final class Terminal implements PrintTerminal {

	/** 登录表达式，格式: OPEN|LOGIN|CONNECT ipv4|ipv6|dns port **/
	private final static String LOGIN_REGEX = "^\\s*(?i)(?:OPEN|CONNECT|LOGIN)\\s+(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}||\\p{XDigit}{1,4}\\:\\p{XDigit}{1,4}\\:\\p{XDigit}{1,4}\\:\\p{XDigit}{1,4}\\:\\p{XDigit}{1,4}\\:\\p{XDigit}{1,4}\\:\\p{XDigit}{1,4}\\:\\p{XDigit}{1,4}|[a-zA-Z0-9]{1}[\\w\\.\\-]{1,})(?:\\s+|\\:)(\\d{1,5})\\s*$";	

	/** JAVA控制台 */
	private Console console;

	/** 是否进入登录状态 **/
	private boolean logined;

	private SQLChecker checker = new SQLChecker();

	/**
	 * 
	 */
	public Terminal() {
		super();
		logined = false;
	}

	/*
	 * 在终端上显示标准信息
	 * @see com.lexst.algorithm.collect.PrintTerminal#showMessage(java.lang.String)
	 */
	@Override
	public void showMessage(String s) {
		System.out.println(s);
	}

	/*
	 * 在终端上显示错误信息
	 * @see com.lexst.algorithm.collect.PrintTerminal#showFault(java.lang.String)
	 */
	@Override
	public void showFault(String s) {
		System.out.println(s);
	}
	
	/*
	 * 在终端上显示异常堆栈信息
	 * @see com.lexst.algorithm.collect.PrintTerminal#showFault(java.lang.Throwable)
	 */
	@Override
	public void showFault(Throwable t) {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
		PrintStream s = new PrintStream(buff, true);
		t.printStackTrace(s);
		byte[] data = buff.toByteArray();
		showFault(new String(data, 0, data.length));
	}

	/*
	 * 在终端上显示表头
	 * @see com.lexst.algorithm.collect.PrintTerminal#showTitle(com.lexst.sql.schema.Sheet)
	 */
	public int showTitle(Sheet sheet) {
		List<String> array = new ArrayList<String>();
		for (ColumnAttribute attribute : sheet.values()) {
			array.add(attribute.getName());
		}
		String[] s = new String[array.size()];
		this.showHead(array.toArray(s));
		return array.size();
	}
	
	/*
	 * 在终端上显示记录
	 * @see com.lexst.algorithm.collect.PrintTerminal#showRow(com.lexst.sql.schema.Sheet, com.lexst.sql.row.Row)
	 */
	public int showRow(Sheet sheet, Row row) {
//		StringBuilder buff = new StringBuilder();			
//		for (short columnId : table.idSet()) {
//			com.lexst.sql.column.Column column = row.find(columnId);
//			String value = LiveUtil.showColumn(table, columnId, column);
//			if (buff.length() > 0) buff.append(" | ");
//			if (value != null) buff.append(value);
//		}
		
		String[] s = LiveUtil.showRow(sheet, row);
		StringBuilder buff = new StringBuilder();
		for (int i = 0; i < s.length; i++) {
			if (buff.length() > 0) buff.append(" | ");
			buff.append(s[i]);
		}

		System.out.println(buff.toString());
		return s.length;
	}
	
	/**
	 * 根据表中的列属性定义显示一行记录
	 * @param table
	 * @param row
	 * @return
	 */
	public int showRow(Table table, Row row) {
		return showRow(table.getSheet(), row);
	}
	
	/**
	 * @return
	 */
	public boolean isLogined() {
		return this.logined;
	}

	/**
	 * @return
	 */
	public Console getConsole() {
		return this.console;
	}

	/**
	 * load system console
	 * @return
	 */
	public boolean initialize() {
		if (console == null) {
			console = System.console();
		}
		return (console != null);
	}
	
	/**
	 * 从终端上接受命令
	 * @return
	 */
	private String input() {
		String cmd = console.readLine("%s", "SQL> ");
		return cmd.trim();
	}
	
	private boolean confirm() {
		while (true) {
			String cmd = console.readLine("%s", "SQL> do it? (Yes or No) ");
			cmd = cmd.trim();
			if ("YES".equalsIgnoreCase(cmd) || "Y".equalsIgnoreCase(cmd)) {
				return true;
			} else if ("NO".equalsIgnoreCase(cmd) || "N".equalsIgnoreCase(cmd)) {
				break;
			}
		}
		return false;
	}



	private void showFault(String format, Object... args) {
		String s = String.format(format, args);
		showFault(s);
	}



	private void showMessage(String format, Object... args) {
		String s = String.format(format, args);
		showMessage(s);
	}

	private void showMessage(String naming, Address[] hosts) {
		StringBuilder buff = new StringBuilder();
		buff.append(String.format("accpeted '%s'", naming));
		for (int i = 0; hosts != null && i < hosts.length; i++) {
			buff.append("\n");
			buff.append(String.format("site: %s", hosts[i].toString()));
		}
		this.showMessage(buff.toString());
	}

	private void showMessage(Space space, Address[] addresses) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("accpeted '%s'", space));
		for (int i = 0; addresses != null && i < addresses.length; i++) {
			sb.append("\n");
			sb.append(String.format("site: %s", addresses[i].toString()));
		}
		this.showMessage(sb.toString());
	}
	
	private boolean isHelp(String cmd) {
		return "HELP".equalsIgnoreCase(cmd);
	}
	
	private boolean isExit(String cmd) {
		return "EXIT".equalsIgnoreCase(cmd) || "QUIT".equalsIgnoreCase(cmd);
	}

	/**
	 * 接受输入命令并且执行操作。<br>
	 * 如果是退出，返回TRUE；否则执行命令返回FALSE。<br>
	 * 
	 * @return
	 */
	public boolean execute() {
		String cmd = input();
		
		if (isExit(cmd)) {
			System.out.println("exit console");
			return true; // 退出命令
		} else if (isHelp(cmd)) {
			this.help(); // 如果是帮助
		} else {
			this.execute(cmd); // 执行其它命令
		}
		return false;
	}
	
	/**
	 * 执行SQL命令或者自定义的类SQL命令
	 * 
	 * @param sql
	 */
	private void execute(String sql) {
		boolean success = false;
		try {
			// 数据库(检测SQL语句正确后需要用户再确认)
			success = checker.isCreateSchema(sql, TouchPool.getInstance());
			if (success && confirm()) createSchema(sql);
			if (!success) {
				success = checker.isDropSchema(sql, TouchPool.getInstance());
				if (success && confirm()) dropSchema(sql);
			}
			if (!success) {
				success = checker.isShowSchema(sql, TouchPool.getInstance());
				if (success) showSchema(sql);
			}
			// 数据库表(检测SQL语句正确后需要用户再确认)
			if(!success) {
				success = checker.isCreateTable(sql, TouchPool.getInstance());
				if(success && confirm()) createTable(sql);
			}
			if (!success) {
				success = checker.isDropTable(sql, TouchPool.getInstance());
				if (success && confirm()) deleteTable(sql);
			}
			if (!success) {
				success = checker.isShowTable(sql, TouchPool.getInstance());
				if (success) showTable(sql);
			}
			// 用户账号
			if(!success) {
				success = checker.isCreateUser(sql, TouchPool.getInstance());
				if(success && confirm()) createUser(sql);
			}
			if(!success) {
				success = checker.isDropUser(sql, TouchPool.getInstance());
				if(success && confirm()) dropUser(sql);
			}
			if(!success) {
				success = checker.isDropSHA1User(sql);
				if(success && confirm()) dropSHA1User(sql);
			}
			if(!success) {
				success = checker.isAlterUser(sql, TouchPool.getInstance());
				if(success && confirm()) alterUser(sql);
			}
			// 用户权限
			if(!success) {
				success = checker.isGrant(sql, TouchPool.getInstance());
				if(success && confirm()) grant(sql);
			}
			if(!success) {
				success = checker.isRevoke(sql, TouchPool.getInstance());
				if(success && confirm()) revoke(sql);
			}
			// SQL操纵命令(SQL语句正确后需要用户再确认)
			if(!success) {
				success = checker.isSelect(sql, TouchPool.getInstance());
				if(success) select(sql);
			}
			if(!success) {
				success = checker.isDelete(sql, TouchPool.getInstance());
				if(success && confirm()) delete(sql);
			}
			if (!success) {
				success = checker.isInsert(sql, TouchPool.getInstance());
				if (success && confirm()) insert(sql);
			}
			if (!success) {
				success = checker.isInject(sql, TouchPool.getInstance());
				if (success && confirm()) inject(sql);
			}			
			if (!success) {
				success = checker.isUpdate(sql, TouchPool.getInstance());
				if (success && confirm()) update(sql);
			}
			// conduct分布计算
			if (!success) {
				success = checker.isConduct(sql, TouchPool.getInstance());
				if (success && confirm()) conduct(sql);
			}
			// 数据块操作
			if(!success) {
				success = checker.isSetChunkSize(sql, TouchPool.getInstance());
				if (success && confirm()) setChunkSize(sql);
			}
			if (!success) {
				success = checker.isShowChunkSize(sql, TouchPool.getInstance());
				if (success) showChunksize(sql);
			}
			if(!success) {
				success = checker.isSwitchTime(sql, TouchPool.getInstance());
				if(success && confirm()) setSwitchTime(sql);
			}
			// 数据和索引的加载/卸载
			if (!success) {
				success = checker.isLoadIndex(sql, TouchPool.getInstance());
				if (success && confirm()) loadIndex(sql);
			}
			if (!success) {
				success = checker.isStopIndex(sql, TouchPool.getInstance());
				if (success && confirm()) stopIndex(sql);
			}
			if (!success) {
				success = checker.isLoadChunk(sql, TouchPool.getInstance());
				if (success && confirm()) loadChunk(sql);
			}
			if (!success) {
				success = checker.isStopChunk(sql, TouchPool.getInstance());
				if (success && confirm()) stopChunk(sql);
			}
			// 数据重组(作用于DATA节点，不改变数据结构)
			if(!success) {
				success = checker.isRebuild(sql, TouchPool.getInstance());
				if(success && confirm()) loadRebuild(sql);
			}
			// 数据重构(作用于BUILD节点，属ETL范畴)
			if(!success) {
				success = checker.isBuildTask(sql);
				if(success && confirm()) buildTask(sql);
			}
			// 其它
			if (!success) {
				success = checker.isShowSite(sql);
				if (success) showSite(sql);
			}
			if (!success) {
				success = checker.isSetCollectPath(sql);
				if (success && confirm()) setCollectPath(sql);
			}
			if (!success) {
				success = checker.isTestCollectPath(sql);
				if (success) testCollectTask(sql);
			}
			if(!success) {
				success = checker.isShowTask(sql);
				if(success) showTask(sql);
			}
			if (!success) {
				showFault("invalid sql command");
			}
		} catch (SQLSyntaxException exp) {
			String msg = exp.getMessage();
			this.showFault(msg);
		}
	}
	
	/**
	 * 
	 * @param sql
	 */
	private void createTable(String sql) {
		TableParser parser = new TableParser();
		Table table = parser.splitCreateTable(sql, true, TouchPool.getInstance());
		
		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().createTable(null, local, table);
		if (success) {
			Space space = table.getSpace();
			this.showMessage("create '%s' success", space);
		} else {
			this.showFault("cannot create '%s'", table.getSpace());
		}
	}

//	private String[] splitTIL(String[] items) {
//		ArrayList<String> array = new ArrayList<String>();
//		for (int index = 0; index < 3; index++) {
//			String item = null;
//			if (index == 0) {
//				for (int j = 0; j < items.length; j++) {
//					if (checker.isCreateTableSyntax(items[j])) {
//						item = items[j]; break;
//					}
//				}
//			} else if (index == 1) {
//				for (int j = 0; j < items.length; j++) {
//					if (checker.isCreateIndexSyntax(items[j])) {
//						item = items[j]; break;
//					}
//				}
//			}
//			if (item == null && (index == 0 || index == 1)) {
//				return null;
//			}
//			if (item != null) array.add(item);
//		}
//		if(array.isEmpty()) return null;
//		String[] all = new String[array.size()];
//		return array.toArray(all);
//	}

	/**
	 * 
	 */
	private void select(String sql) {
		SelectParser parser = new SelectParser();
		Select select = parser.split(sql, TouchPool.getInstance());
		
		SiteHost remote = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();

		// call "select * from database.table where ..."
		SQLCaller caller = new SQLCaller();
		byte[] bytes = caller.select(remote, local, select);

		// 显示检索记录
		Table table = TouchPool.getInstance().findTable(select.getSpace());
		// 显示信息
		this.splitShow(table, bytes);
	}

//	/**
//	 * "DIRECT"数据检索
//	 * 
//	 * @param sql
//	 */
//	private void direct(String sql) {
//		// 解析分布计算语句
//		DirectParser parser = new DirectParser();
//		Direct direct = parser.split(sql, JobPool.getInstance());
//
//		SiteHost remote = JobPool.getInstance().getRemote();
//		LiveSite local = Launcher.getInstance().getLocal();
//		
//		// 发起DC命令
//		SQLCaller caller = new SQLCaller();
//		byte[] data = caller.dc(remote, local, direct);
//
//		// 启动本地接口，做后续操作
//		CollectTask collTask = null;
//		CollectObject collect = direct.getCollect();
//		if (collect != null) {
//			collTask = CollectTaskPool.getInstance().find(collect.getNaming());
//		}
//		// 没有定义接口，启动默认
//		if (collTask == null) {
//			collTask = new DefaultCollectTask();
//		}
//		// 在终端上打印数据
//		collTask.display(direct, JobPool.getInstance().getTables(), this, data, 0, data.length);
//	}
	
	/**
	 * CONDUCT数据检索
	 * 
	 * @param sql
	 */
	private void conduct(String sql) {
		ConductParser parser = new ConductParser();
		Conduct conduct = parser.split(sql, TouchPool.getInstance());
		
		SiteHost remote = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();

		// 发起conduct检索命令
		SQLCaller caller = new SQLCaller();
		byte[] data = caller.conduct(remote, local, conduct);

		// 根据命名，查找对应的显示接口
		CollectTask collTask = null;
		CollectObject collect = conduct.getCollect();
		if (collect != null) {
			collTask = CollectTaskPool.getInstance().find(collect.getNaming());
		}
		// 没有定义接口，启动默认
		if (collTask == null) {
			collTask = new DefaultCollectTask();
		}
		// 数据显示
		try {
			collTask.display(conduct, TouchPool.getInstance().getTables(),
					this, data, 0, data.length);
		} catch (CollectTaskException e) {
			this.showFault(e);
		}
	}

	/**
	 * SQL删除数据
	 * @param sql
	 */
	private void delete(String sql) {
		DeleteParser parser = new DeleteParser();
		Delete delete = parser.split(sql, TouchPool.getInstance());
		
		SiteHost top = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();

		// show delete count		
		SQLCaller caller = new SQLCaller();
		long count = caller.delete(top, local, delete);
		
		// show delete items
		this.showMessage("delete count: %d", count);
		
	}

	/**
	 * SQL插入数据(单行记录)
	 * 
	 * @param sql
	 */
	private void insert(String sql) {
		InsertParser parser = new InsertParser();
		Insert insert = parser.splitInsert(sql, TouchPool.getInstance());
		
		SiteHost top = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();

		SQLCaller caller = new SQLCaller();
		int count = caller.insert(top, local, insert);

		if(count > 0) {
			this.showMessage("insert %d item", count);
		} else {
			this.showFault("insert failed");
		}
	}

	/**
	 * SQL插入数据(多行)
	 * 
	 * @param sql
	 */
	private void inject(String sql) {
		InsertParser parser = new InsertParser();
		Inject inject = parser.splitInject(sql, TouchPool.getInstance());
		
		SiteHost top = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();

		SQLCaller caller = new SQLCaller();
		int count = caller.inject(top, local, inject);

		if(count > 0) {
			this.showMessage("inject %d item", count);
		} else {
			this.showFault("inject failed");
		}
	}

	/**
	 * SQL 更新数据
	 * @param sql
	 */
	private void update(String sql) {
		UpdateParser parser = new UpdateParser();
		Update update = parser.split(sql, TouchPool.getInstance());
		
		SiteHost top = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();
		
		// show delete count		
		SQLCaller caller = new SQLCaller();
		long count = caller.update(top, local, update);
		
		this.showMessage("update count: %d", count);
	}
	
	private void showTable(String sql) {
		TableParser parser = new TableParser();
		Space space = parser.splitShowTable(sql, true, TouchPool.getInstance());
		
		LiveSite local = Launcher.getInstance().getLocal();
		Table table = TouchPool.getInstance().findTable(space);
		if (table == null) {
			table = TouchPool.getInstance().findTable(null, local, space);
		}
		if (table == null) {
			this.showFault("cannot find '%s'", space);
			return;
		}
		
		StringBuilder buff = new StringBuilder();
		buff.append(String.format("%s\n", table.getSpace()));
		for(ColumnAttribute attribute : table.values()) {			
			String s = null;
			if(Type.isWord( attribute.getType() )) {
				s = String.format("%s | %s | %s | %s | %s | %s\r\n", 
					attribute.getName(),
					Type.showDataType(attribute.getType()),
					Type.showIndexType(attribute.getKey()),
					((WordAttribute)attribute).isSentient() ? "Case Sentient" : "Not Case",
					((WordAttribute)attribute).isLike() ? "Like" : "Not Like",
					attribute.isNullable() ? "Null" : "Not Null");
			} else {
				s = String.format("%s | %s | %s | %s\r\n", attribute.getName(), Type.showDataType(attribute.getType()), 
					Type.showIndexType(attribute.getKey()),
					attribute.isNullable() ? "Null" : "Not Null");
			}
			buff.append(s);
		}
		System.out.print(buff.toString());
	}

	/**
	 * delete sql table
	 * @param sql
	 * @return
	 */
	private void deleteTable(String sql) {
		TableParser parser = new TableParser();
		Space space = parser.splitDropTable(sql, true, TouchPool.getInstance());
		
		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().deleteTable(null, local, space);
		if (success) {
			this.showMessage("delete '%s' success", space);
		} else {
			this.showFault("cannot delete '%s'", space);
		}
	}

	/**
	 * 定义数据重构时间
	 * 
	 * @param sql
	 */
	private void setSwitchTime(String sql) {
		SwitchTimeParser parser = new SwitchTimeParser();
		SwitchTime st = parser.split(sql, TouchPool.getInstance());

		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().setRebuildTime(null, local,
				st.getSpace(), st.getColumnId(),
				st.getType(), st.getInterval());
		if (success) {
			this.showMessage("set success");
		} else {
			this.showFault("set fault");
		}
	}
	
	/**
	 * 建立一个用户账号
	 * @param sql
	 */
	private void createUser(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();
		UserParser parser = new UserParser();
		User user = parser.splitCreateUser(sql, true, TouchPool.getInstance());
		boolean success = TouchPool.getInstance().createUser(null, local, user);
		if(success) {
			this.showMessage("create user success");
		} else {
			this.showFault("cannot create user");
		}
	}
	
	/**
	 * 删除用户账号以及账号下的所有数据库记录
	 * 
	 * @param sql
	 */
	private void dropUser(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();
		UserParser parser = new UserParser();
		User user = parser.splitDropUser(sql, true, TouchPool.getInstance());
		boolean success = TouchPool.getInstance().deleteUser(null, local, user.getHexUsername());
		if(success) {
			this.showMessage("drop user success");
		} else {
			this.showFault("cannot drop user");
		}
	}
	
	private void dropSHA1User(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();
		UserParser parser = new UserParser();
		User user = parser.splitDropSHA1User(sql);
		boolean success = TouchPool.getInstance().deleteUser(null, local, user.getHexUsername());
		if(success) {
			this.showMessage("drop sha1 user success");
		} else {
			this.showFault("cannot drop sha1 user");
		}
	}

	/**
	 * 修改用户账号
	 * @param sql
	 */
	private void alterUser(String sql) {
		UserParser parser = new UserParser();
		User user = parser.splitAlterUser(sql, true, TouchPool.getInstance());

		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().alterUser(null, local, user);
		if (success) {
			this.showMessage("alter user success");
		} else {
			this.showFault("cannot alter user");
		}
	}

	private void grant(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();
		GrantParser parser = new GrantParser();
		Permit permit = parser.split(sql, true, TouchPool.getInstance());
		boolean success = TouchPool.getInstance().addPermit(null, local, permit);
		if(success) {
			this.showMessage("grant success");
		} else {
			this.showFault("grant fault");
		}
	}

	private void revoke(String sql) {
		RevokeParser parser = new RevokeParser();
		Permit permit = parser.split(sql, true, TouchPool.getInstance());

		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().deletePermit(null, local, permit);
		if (success) {
			this.showMessage("revoke success");
		} else {
			this.showFault("revoke fault");
		}
	}

	private void dropSchema(String sql) {
		LiveSite site = Launcher.getInstance().getLocal();
		SchemaParser parser = new SchemaParser();
		Schema schema = parser.splitDropSchema(sql, true, TouchPool.getInstance());
		boolean success = TouchPool.getInstance().deleteSchema(null, site, schema.getName());
		if(success) {
			this.showMessage("drop '%s' success", schema.getName());
		} else {
			this.showFault("cannot drop '%s' database");
		}
	}
	
	private void createSchema(String sql) {
		LiveSite site = Launcher.getInstance().getLocal();
		
		SchemaParser parser = new SchemaParser();
		Schema db = parser.splitCreateSchema(sql, true, TouchPool.getInstance());
		boolean success = TouchPool.getInstance().createSchema(null, site, db);
		if (success) {
			this.showMessage("create '%s' success", db.getName());
		} else {
			this.showFault("cannot create database");
		}
	}
	
	private void showSchema(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();
		SchemaParser parser = new SchemaParser();
		String db = parser.splitShowSchema(sql, true, TouchPool.getInstance());

		Schema[] schemas = null;
		if ("all".equalsIgnoreCase(db)) {
			schemas = TouchPool.getInstance().findAllSchema(null, local);
		} else {
			Schema schema = TouchPool.getInstance().findSchema(null, local, db);
			if (schema != null) {
				schemas = new Schema[] { schema };
			}
		}
		if (schemas == null || schemas.length == 0) {
			System.out.printf("cannot find %s\r\n", db);
			return;
		}

		StringBuilder buff = new StringBuilder();
		for (Schema schema : schemas) {
			if (buff.length() > 0) buff.append("\r\n");
			buff.append(String.format("%s\n", schema.getName()));
			buff.append(LiveUtil.format_size("maxsize", schema.getMaxSize()) + "\n");
			for (Table table : schema.listTable()) {
				String s = String.format("--- %s ---\r\n", table.getSpace());
				buff.append(s);
			}
		}
		System.out.print(buff.toString());
	}
	
	private void setChunkSize(String sql) {
		ChunkParser parser = new ChunkParser();
		Object[] objects = parser.splitSetChunkSize(sql, true, TouchPool.getInstance());
		Space space = (Space) objects[0];
		Integer size = (Integer) objects[1];
		
		if (size > LiveUtil.G) {
			this.showFault("chunk size is too large");
			return;
		}
		
		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().setChunkSize(null, local, space, size.intValue());
		if(success) {
			this.showMessage("set success");
		} else {
			this.showFault("set fault");
		}
	}
	
	/**
	 * 重构数据块
	 * 
	 * @param sql
	 */
	public void loadRebuild(String sql) {
		RebuildParser parser = new RebuildParser();
		RebuildHostResult host = parser.split(sql, TouchPool.getInstance());
		
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().rebuild(null, local, host.getSpace(), host.getColumnId(), host.getAddresses());
			this.showMessage(host.getSpace(), s);
		} catch (VisitException exp) {
			this.showFault("cannot accepted rebuild '%s'", host.getSpace());
			Logger.error(exp);
		}
	}
	
	/**
	 * 加载索引
	 * @param sql
	 */
	private void loadIndex(String sql) {
		IndexParser parser = new IndexParser();
		RebuildHostResult sh = parser.splitLoadIndex(sql, true, TouchPool.getInstance());
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().loadIndex(null, local, sh.getSpace(), sh.getAddresses());
			this.showMessage(sh.getSpace(), s);
		} catch (VisitException exp) {
			this.showFault("cannot accpeted index '%s'", sh.getSpace());
		}
	}

	private void stopIndex(String sql) {
		IndexParser parser = new IndexParser();
		RebuildHostResult sh = parser.splitStopIndex(sql, true, TouchPool.getInstance());
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().stopIndex(null, local, sh.getSpace(), sh.getAddresses());
			this.showMessage(sh.getSpace(), s);
		} catch (VisitException exp) {
			this.showFault("cannot accpeted index '%s'", sh.getSpace());
		}
	}
	
	/**
	 * 根据数据库表，加载数据块(可指定DATA主机地址)
	 * @param sql
	 */
	private void loadChunk(String sql) {
		ChunkParser parser = new ChunkParser();
		RebuildHostResult host = parser.splitLoadChunk(sql, true, TouchPool.getInstance());
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().loadChunk(null, local, host.getSpace(), host.getAddresses());
			this.showMessage(host.getSpace(), s);
		} catch (VisitException exp) {
			this.showFault("cannot accpeted index '%s'", host.getSpace());
		}
	}
	
	/**
	 * 根据数据库表，卸载数据块(可指定DATA主机地址)
	 * @param sql
	 */
	private void stopChunk(String sql) {
		ChunkParser parser = new ChunkParser();
		RebuildHostResult host = parser.splitStopChunk(sql, true, TouchPool.getInstance());
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().stopChunk(null, local, host.getSpace(), host.getAddresses());
			this.showMessage(host.getSpace(), s);
		} catch (VisitException exp) {
			this.showFault("cannot accpeted chunk '%s'", host.getSpace());
		}
	}
	
	private void buildTask(String sql) {
		BuildTaskParser parser = new BuildTaskParser();
		NamingHostResult host = parser.split(sql);
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().buildTask(null, local, host.getNaming(), host.getAddresses());
			this.showMessage(host.getNaming(), s);
		} catch (VisitException exp) {
			this.showFault("cannot accpeted naming task '%s'", host.getNaming());
		}
	}
	
	private void showChunksize(String sql) {
		ChunkParser parser = new ChunkParser();
		ChunkHostResult host = parser.splitShowChunkSize(sql, true, TouchPool.getInstance());
		LiveSite local = Launcher.getInstance().getLocal();
		
		long chunksize = 0L;
		try {
			if (host.getSpace() != null) {
				chunksize = TouchPool.getInstance().showChunkSize(null, local.getHost(), host.getSpace(), host.getHostType(), host.getAddresses());
			} else if (host.getSchema() != null) {
				chunksize = TouchPool.getInstance().showChunkSize(null, local.getHost(), host.getSchema(), host.getHostType(), host.getAddresses());
			}
		} catch (VisitException exp) {
			this.showMessage("show chunksize, failed!");
			return;
		}

		String s = LiveUtil.format_size("chunk-size", chunksize);
		this.showMessage(s);
	}
	
	private void showSite(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();
		
		ShowSiteParser parser = new ShowSiteParser();
		ShowSiteResult result = parser.split(sql);
		
		SiteHost[] sites = null;
		try {
			sites = TouchPool.getInstance().showSite(null, local, result.getSiteFimaly(), result.getAddresses());
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		
		if (sites == null || sites.length ==0) {
			this.showFault("cannot find site");
			return;
		}
		
		Table head = new Table();
		head.add(new com.lexst.sql.column.attribute.CharAttribute((short) 1, "IP"));
		head.add(new com.lexst.sql.column.attribute.IntegerAttribute((short) 2, "TCP PORT", 0));
		head.add(new com.lexst.sql.column.attribute.IntegerAttribute((short) 3, "UDP PORT", 0));
		this.showHead(head);
		
		for (SiteHost site : sites) {
			showMessage("%s | %d | %d", site.getSpecifyAddress(), site.getTCPort(), site.getUDPort());
		}
	}
	
	private void setCollectPath(String sql) {
		CollectParser parser = new CollectParser();
		String path = parser.splitCollectPath(sql);
		// load collect configure
		List<String> list = CollectTaskPool.getInstance().load(path);
		int items = (list == null ? 0 : list.size());
		StringBuilder buff = new StringBuilder();
		for (int i = 0; list != null && i < list.size(); i++) {
			buff.append(String.format("naming: %s\r\n", list.get(i)));
		}
		showMessage("load collect resource from %s, item count:%d\r\n%s", path, items, buff.toString());
	}

	private void testCollectTask(String sql) {
		CollectParser parser = new CollectParser();
		String naming = parser.splitCollectTask(sql);
		// find task
		CollectTask task = CollectTaskPool.getInstance().find(naming);
		// show result
		if(task == null) {
			this.showFault("cannot find '%s'!", naming);
		} else {
			this.showMessage("'%s' existed!", naming);
		}
	}
	
	private void showTask(String sql) {
		ShowTaskParser parser = new ShowTaskParser();
		TaskHostResult host = parser.splitShowTask(sql);
		String tag = host.getTag(); //keyword: diffuse,aggregate,collect,build,all
		
		List<TaskAddress> array = new ArrayList<TaskAddress>();
		if ("collect".equalsIgnoreCase(tag) || "all".equalsIgnoreCase(tag)) {
			Set<Naming> s = CollectTaskPool.getInstance().getNamings();
			for (Naming naming : s) {
				array.add(new TaskAddress(naming.toString(), "localhost"));
			}
		} 
		if (!"collect".equalsIgnoreCase(tag) || "all".equalsIgnoreCase(tag)) {
			LiveSite local = Launcher.getInstance().getLocal();
			try {
				List<TaskAddress> list = TouchPool.getInstance().showTask(null, local.getHost(), tag,  host.getAddresses());
				if(list != null) array.addAll(list);
			} catch (VisitException exp) {
				showMessage("find task naming, failed!");
				return;
			}
		}
		
		// sort
		if (!array.isEmpty()) {
			java.util.Collections.sort(array);
		}
		
		showMessage("naming | address");
		for(TaskAddress naming: array) {
			showMessage("%s | %s", naming.getNaming(), naming.getAddress());			
		}
	}	

	/**
	 * @param columns
	 */
	private void showHead(String[] columns) {
		StringBuilder buff = new StringBuilder();
		for (int i = 0; columns != null && i < columns.length; i++) {
			if (buff.length() > 0) buff.append(" | ");
			buff.append(columns[i]);
		}
		System.out.println(buff.toString());
	}

	/**
	 * @param table
	 */
	private void showHead(Table table) {
		List<String> array = new ArrayList<String>();
		for (ColumnAttribute attribute : table.values()) {
			String s = attribute.getName();
			array.add(s);
		}
		String[] all = new String[array.size()];
		this.showHead(array.toArray(all));
	}
		
	private void splitShow(Table head, byte[] data) {
		if(data == null) return;

		int off = 0, size = data.length;
		if(size < 8) return;
		int items = Numeric.toInteger(data, off, 8);
		off += 8;

		if (head == null) {
			System.out.printf("rows %d\n", items);
			return;
		}
		
		this.showHead(head);

		// split and show
		while(off < size) {
			Row row = new Row();
			int len = row.resolve(head, data, off, size - off);
			if(len < 1) break;
			off += len;
			this.showRow(head, row);
		}
	}

	/**
	 * 连接TOP节点
	 * @param remote
	 * @return
	 */
	private boolean connect(SiteHost remote) {
		String username = console.readLine("%s", "Username: ");
		char[] pwd = console.readPassword("%s", "Password: ");
		String password = new String(pwd);
		//choose a ciphertext
		int digit = 0;
		while (true) {
			String num = console.readLine("%s","0 None | 1 AES | 2 DES | 3 3DES | 4 BLOWFISH | 5 MD5 | 6 SHA1 \nPlease choose:");
			try {
				digit = Integer.parseInt(num);
				if (0 <= digit && digit <= 6) break;
			} catch (NumberFormatException exp) {

			}
		}
 
		LiveSite site = Launcher.getInstance().getLocal();
		site.setUser(username, password);
		// set secure algorithm
		switch(digit) {
		case 1:
			site.setAlgorithm(Cipher.translate(Cipher.AES)); break;
		case 2: 
			site.setAlgorithm(Cipher.translate(Cipher.DES)); break;
		case 3: 
			site.setAlgorithm(Cipher.translate(Cipher.DES3)); break;
		case 4:
			site.setAlgorithm(Cipher.translate(Cipher.BLOWFISH)); break;
		case 5:
			site.setAlgorithm(Cipher.translate(Cipher.MD5)); break;
		case 6:
			site.setAlgorithm(Cipher.translate(Cipher.SHA1)); break;
		}

		// 登陆到TOP服务器
		logined = TouchPool.getInstance().login(remote, site);
		System.out.printf("%s\n", (logined ? "login success" : "login failed"));
		return logined;
	}
	
	/**
	 * 登录
	 * @return
	 */
	public boolean login() {
		boolean success = false;
		while (true) {
			String cmd = input();
			if (isHelp(cmd)) {
				this.help();
			} else if (isExit(cmd)) {
				System.out.println("exit console");
				break;
			} else {
				Pattern pattern = Pattern.compile(Terminal.LOGIN_REGEX);
				Matcher matcher = pattern.matcher(cmd);
				if (!matcher.matches()) {
					System.out.println("please login!  [OPEN|LOGIN|CONNECT] address port");
					break;
				}
				
				System.out.printf("[%s] [%s]\n", matcher.group(1), matcher.group(2));
				
				try {
					Address address = new Address(matcher.group(1));
					int port = Integer.parseInt(matcher.group(2));
					SiteHost host = new SiteHost(address, port, port);
					success = connect(host);
				} catch (IOException e) {
					System.out.println("address error! please login...");
				}

				if(success) break;
			}
		}
		return success;
	}

	/**
	 * print sql command
	 */
	private void help() {
		System.out.println("----------- command -----------");
		LiveStreamInvoker invoker = new LiveStreamInvoker();
		String s = invoker.help();
		showMessage(s);
	}
	
}