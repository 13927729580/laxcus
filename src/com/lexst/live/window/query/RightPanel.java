/**
 *
 */
package com.lexst.live.window.query;

import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.List;

import javax.swing.*;
import javax.swing.border.*;

import com.lexst.algorithm.collect.*;
import com.lexst.live.*;
import com.lexst.live.pool.*;
import com.lexst.live.window.*;
import com.lexst.log.client.*;
import com.lexst.site.live.*;
import com.lexst.sql.*;
import com.lexst.sql.account.*;
import com.lexst.sql.charset.*;
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
import com.lexst.util.res.*;
import com.lexst.visit.*;

public class RightPanel extends JPanel implements ActionListener {

	private static final long serialVersionUID = 1L;

	private JButton cmdCheck = new JButton();
	private JButton cmdGo = new JButton();
	private JButton cmdFont = new JButton();
	private JButton cmdCut = new JButton();
	private JButton cmdClear = new JButton();

	private JToolBar toolbar = new JToolBar();

	private SQLTextPane txtSQL = new SQLTextPane();
	private TabPanel controls = new TabPanel();

	private SQLChecker checker = new SQLChecker();

	/**
	 *
	 */
	public RightPanel() {
		super();
	}

	public class SQLKeyAdapter extends KeyAdapter {
		
		public void keyPressed(KeyEvent e) {
			if (e.getSource() == txtSQL) {
				switch (e.getKeyCode()) {
				case KeyEvent.VK_F5:
					check();
					break;
				case KeyEvent.VK_F6:
					execute();
					break;
				}
			}
		}
		
		public void keyReleased(KeyEvent e) {
			int size = 0;
			if (e.getModifiersEx() == (KeyEvent.SHIFT_DOWN_MASK | KeyEvent.CTRL_DOWN_MASK)) {
				switch (e.getKeyChar()) {
				case '<':
					size = -1; break;
				case '>':
					size = 1; break;
				}
			}
			
			if(size > 0) {
				size = txtSQL.getFont().getSize();
				size = (size < 32 ? size + 1 : 0);
			} else if(size < 0) {
				size = txtSQL.getFont().getSize();
				size = (size > 12 ? size - 1 : 0);
			}
			if (size != 0) {
				Font font = txtSQL.getFont();
				font = new Font(font.getName(), font.getStyle(), size);
				txtSQL.setFont(font);
			}
		}
	}
	
	public LogPrinter getLogPrinter() {
		return controls.getLogPrinter();
	}

	private void selectFont() {
		Font defont = txtSQL.getFont();
		Font font = FontDialog.showDialog(Launcher.getInstance().getFrame(), true, defont);
		if (font != null) {
			txtSQL.setFont(font);
		}
	}

	/* (non-Javadoc)
	 * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		if (e.getSource() == cmdCheck) {
			this.check();
		} else if (e.getSource() == cmdGo) {
			this.execute();
		} else if (e.getSource() == cmdFont) {
			this.selectFont();
		} else if (e.getSource() == cmdCut) {
			controls.deleteTip();
		} else if (e.getSource() == cmdClear) {
			controls.clearTable();
		}
	}
	
	/**
	 * 
	 */
	public void check() {
		String sql = txtSQL.getText().trim();
		if (sql.length() == 0) return;

		if ("HELP".equalsIgnoreCase(sql)) {
			controls.showMessage("correct syntax");
			return;
		}

		boolean success = false;
		try {			
			success = checker.isCreateSchema(sql, TouchPool.getInstance());
			if (!success) success = checker.isShowSchema(sql, TouchPool.getInstance());
			if (!success) success = checker.isDropSchema(sql, TouchPool.getInstance());
			
			if (!success) success = checker.isCreateUser(sql, TouchPool.getInstance());
			if (!success) success = checker.isDropUser(sql, TouchPool.getInstance());
			if (!success) success = checker.isDropSHA1User(sql);
			if (!success) success = checker.isAlterUser(sql, TouchPool.getInstance());
			
			if (!success) success = checker.isGrant(sql, TouchPool.getInstance());
			if (!success) success = checker.isRevoke(sql, TouchPool.getInstance());
			
			if (!success) success = checker.isCreateTable(sql, TouchPool.getInstance());
			if (!success) success = checker.isDropTable(sql, TouchPool.getInstance());
			if (!success) success = checker.isShowTable(sql, TouchPool.getInstance());
			
			if (!success) success = checker.isSetChunkSize(sql, TouchPool.getInstance());
			if (!success) success = checker.isSwitchTime(sql, TouchPool.getInstance());
			if (!success) success = checker.isShowChunkSize(sql, TouchPool.getInstance());
			
			if (!success) success = checker.isLoadIndex(sql, TouchPool.getInstance());
			if (!success) success = checker.isStopIndex(sql, TouchPool.getInstance());
			if (!success) success = checker.isLoadChunk(sql, TouchPool.getInstance());
			if (!success) success = checker.isStopChunk(sql, TouchPool.getInstance());
			if (!success) success = checker.isRebuild(sql, TouchPool.getInstance());
			if (!success) success = checker.isBuildTask(sql);
			
			if (!success) success = checker.isShowSite(sql);
			if (!success) success = checker.isSetCollectPath(sql);
			if (!success) success = checker.isTestCollectPath(sql);
			if (!success) success = checker.isShowTask(sql);
			
			// 判断SQL SELECT语句
			if (!success) success = checker.isSelect(sql, TouchPool.getInstance());
			// 判断SQL "DELETE FROM"语句
			if (!success) success = checker.isDelete(sql, TouchPool.getInstance());
			// 判断SQL "INSERT INTO"
			if(!success) success = checker.isInsert(sql, TouchPool.getInstance());
			// SQL "INJECT INTO"
			if(!success) success = checker.isInject(sql, TouchPool.getInstance());
			// SQL "UPDATE ... SET ..."
			if (!success) success = checker.isUpdate(sql, TouchPool.getInstance());
			// "CONDUCT ..."
			if(!success) success = checker.isConduct(sql, TouchPool.getInstance());
		} catch (SQLSyntaxException exp) {
			String msg = exp.getMessage();
			controls.showFault(msg);
			return;
		}
		
		if (success) {
			controls.showMessage("correct syntax");
		} else {
			controls.showFault("incorrect syntax");
		}
	}


	private void createSchema(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();
		
		SchemaParser parser = new SchemaParser();
		Schema schema = parser.splitCreateSchema(sql, true, TouchPool.getInstance());
		boolean success = TouchPool.getInstance().createSchema(null, local, schema);
		if (success) {
			controls.showMessage("create '%s' success", schema.getName());
			Launcher.getInstance().getFrame().refresh();
		} else {
			controls.showFault("cannot create database");
		}
	}

	/**
	 * @param sql
	 */
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
			controls.showFault("cannot find %s", db);
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

		controls.showMessage(buff.toString());
	}

	/**
	 * 删除当前账号下的一个数据库，以及数据库下的所有记录
	 * @param sql
	 */
	private void dropSchema(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();
		
		SchemaParser parser = new SchemaParser();
		Schema schema = parser.splitDropSchema(sql, true, TouchPool.getInstance());
		boolean success = TouchPool.getInstance().deleteSchema(null, local, schema.getName());
		if(success) {
			controls.showMessage("drop '%s' success", schema.getName());
			Launcher.getInstance().getFrame().refresh();
		} else {
			controls.showFault("cannot drop '%s' database");
		}
	}

	/**
	 * 建立一个用户账号
	 * @param sql
	 */
	private void createUser(String sql) {
		UserParser parser = new UserParser();
		User user = parser.splitCreateUser(sql, true, TouchPool.getInstance());
		
		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().createUser(null, local, user);
		if (success) {
			controls.showMessage("create user success");
		} else {
			controls.showFault("cannot create user");
		}
	}
	
	/**
	 * 删除账号以及账号下所有配置权限、数据库等
	 * @param sql
	 */
	private void dropUser(String sql) {
		UserParser parser = new UserParser();
		User user = parser.splitDropUser(sql, true, TouchPool.getInstance());
		
		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().deleteUser(null, local, user.getHexUsername());
		if (success) {
			controls.showMessage("drop user success");
		} else {
			controls.showFault("cannot drop user");
		}
	}
	
	/**
	 * 删除账号(SHA1格式)以及账号下的所有配置权限、数据库等
	 * @param sql
	 */
	private void dropSHA1User(String sql) {
		UserParser parser = new UserParser();
		User user = parser.splitDropSHA1User(sql);
		
		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().deleteUser(null, local, user.getHexUsername());
		if (success) {
			controls.showMessage("drop user success");
		} else {
			controls.showFault("cannot drop user");
		}
	}

	/**
	 * 修改账号密码
	 * @param sql
	 */
	private void alterUser(String sql) {
		UserParser parser = new UserParser();
		User user = parser.splitAlterUser(sql, true, TouchPool.getInstance());

		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().alterUser(null, local, user);
		if (success) {
			controls.showMessage("alter user success");
		} else {
			controls.showFault("cannot alter user");
		}
	}

	private void grant(String sql) {
		GrantParser parser = new GrantParser();
		Permit permit = parser.split(sql, true, TouchPool.getInstance());

		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().addPermit(null, local, permit);
		if (success) {
			controls.showMessage("grant success");
		} else {
			controls.showFault("grant fault");
		}
	}

	private void revoke(String sql) {
		RevokeParser parser = new RevokeParser();
		Permit permit = parser.split(sql, true, TouchPool.getInstance());

		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().deletePermit(null, local,
				permit);
		if (success) {
			controls.showMessage("revoke success");
		} else {
			controls.showFault("revoke fault");
		}
	}
	
	private void setChunkSize(String sql) {
		ChunkParser parser = new ChunkParser();
		Object[] objects = parser.splitSetChunkSize(sql, true, TouchPool.getInstance());
		Space space = (Space) objects[0];
		Integer size = (Integer) objects[1];
		
		if (size > LiveUtil.G) {
			controls.showFault("chunk-size is too large");
			return;
		}
		
		LiveSite local = Launcher.getInstance().getLocal();
		boolean success = TouchPool.getInstance().setChunkSize(null, local, space, size.intValue());
		if(success) {
			controls.showMessage("set success");
		} else {
			controls.showFault("set fault");
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
				st.getSpace(), st.getColumnId(), st.getType(),
				st.getInterval());
		if (success) {
			controls.showMessage("set success");
		} else {
			controls.showFault("set fault");
		}
	}

	/**
	 * 建立数据库表
	 * @param sql
	 * @param sqlIndex
	 */
	private void createTable(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();
		
		TableParser parser = new TableParser();
		Table table = parser.splitCreateTable(sql, true, TouchPool.getInstance());
		boolean success = TouchPool.getInstance().createTable(null, local, table);
		if (success) {
			controls.showMessage("create '%s' success", table.getSpace());
			Launcher.getInstance().getFrame().refresh();
		} else {
			controls.showFault("cannot create '%s'", table.getSpace());
		}
	}
	
	private void showTable(String sql) {
		TableParser parser = new TableParser();
		Space space = parser.splitShowTable(sql, true, TouchPool.getInstance());
		
		LiveSite local = Launcher.getInstance().getLocal();
		Table table = TouchPool.getInstance().findTable(space);
		// when not found table, query...
		if (table == null) {
			table = TouchPool.getInstance().findTable(null, local, space);
		}
		if (table == null) {
			controls.showFault("cannot find %s", space);
			return;
		}

		// show table configure
		StringBuilder buff = new StringBuilder();
		for(ColumnAttribute field : table.values()) {
			String s = null;
			if(Type.isWord( field.getType() )) {
				s = String.format("%s | %s | %s | %s | %s | %s\r\n", 
					field.getName(),
					Type.showDataType(field.getType()),
					Type.showIndexType(field.getKey()),
					((WordAttribute)field).isSentient() ? "Case Sentient" : "Not Case",
					((WordAttribute)field).isLike() ? "Like" : "Not Like",
					field.isNullable() ? "Null" : "Not Null");
			} else {
				s = String.format("%s | %s | %s | %s\r\n", field.getName(), Type.showDataType(field.getType()), 
					Type.showIndexType(field.getKey()),
					field.isNullable() ? "Null" : "Not Null");
			}
			buff.append(s);
		}
		controls.showMessage(buff.toString());
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
			controls.showMessage("delete '%s' success", space);
			Launcher.getInstance().getFrame().refresh();
		} else {
			controls.showFault("cannot delete '%s'", space);
		}
	}

	/**
	 * query data
	 * @param sql
	 */
	private void select(String sql) {
		SelectParser parser = new SelectParser();
		Select select = parser.split(sql, TouchPool.getInstance());

		SiteHost remote = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();
		
		// 执行SELECT检索		
		SQLCaller caller = new SQLCaller();
		byte[] data = caller.select(remote, local, select);
		// 更新显示表头和设置焦点
		Table table = TouchPool.getInstance().findTable(select.getSpace());
		controls.updateTable(table);
		controls.focusItem();
		// 显示记录
		this.splitShow(table, data);
	}

//	private void direct(String sql) {
//		DirectParser parser = new DirectParser();
//		Direct direct = parser.split(sql, JobPool.getInstance());
//		
//		SiteHost remote = JobPool.getInstance().getRemote();
//		LiveSite local = Launcher.getInstance().getLocal();
//
//		// 启动DC检索
//		SQLCaller caller = new SQLCaller();
//		byte[] data = caller.dc(remote, local, direct);
//
//		// 查找处理接口
//		CollectTask collTask = null;
//		CollectObject collect = direct.getCollect();
//		if (collect != null) {
//			collTask = CollectTaskPool.getInstance().find(collect.getNaming());
//		}
//		if (collTask == null) {
//			collTask = new DefaultCollectTask();
//		}
//		// 显示信息
//		collTask.display(direct, JobPool.getInstance().getTables(), controls, data, 0, data.length);
//	}

	/**
	 * @param sql
	 */
	private void conduct(String sql) {
		ConductParser parser = new ConductParser();
		Conduct conduct = parser.split(sql, TouchPool.getInstance());

		SiteHost remote = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();

		// 启动 conduct 计算
		SQLCaller caller = new SQLCaller();
		byte[] data = caller.conduct(remote, local, conduct);

		// 查找显示接口
		CollectTask collTask = null;
		CollectObject collect = conduct.getCollect();
		if (collect != null) {
			collTask = CollectTaskPool.getInstance().find(collect.getNaming());
		}
		if (collTask == null) {
			collTask = new DefaultCollectTask();
		}
		// 显示信息
		try {
			collTask.display(conduct, TouchPool.getInstance().getTables(), controls, data, 0, data.length);
		} catch (CollectTaskException e) {
			controls.showFault(e);
		}
	}

	private void splitShow(Table table, byte[] data) {
		if(data == null) return;
		
		int off = 0, size = data.length;
		if(size < 8) return;
		int items = Numeric.toInteger(data, off, 8);
		off += 8;

		if(table == null) {
			table = new Table();
			IntegerAttribute field = new IntegerAttribute((short)1, "rows", 0);
			table.add(field);
			controls.updateTable(table);
			
			Row row = new Row();
			com.lexst.sql.column.Integer i = new com.lexst.sql.column.Integer((short)1, items);
			row.add(i);
			controls.addItem(table, row);
			return;
		}
		
		// split and show
		while(off < size) {
			Row row = new Row();
			int len = row.resolve(table, data, off, size - off);
			if(len < 1) break;
			off += len;
			controls.addItem(table, row);
		}
	}

	/**
	 * delete data
	 * @param sql
	 */
	private void delete(String sql) {
		DeleteParser parser = new DeleteParser();
		Delete delete = parser.split(sql, TouchPool.getInstance());
		
		SiteHost remote = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();

		// show delete count
		SQLCaller caller = new SQLCaller();
		long count = caller.delete(remote, local, delete);
		
		// value
		com.lexst.sql.column.Long value = new com.lexst.sql.column.Long((short) 1, count);
		Row row = new Row();
		row.add(value);
		
		// show table head
		String[] head = { "count" };
		controls.updateTable(head);
		// show delete count
		Sheet sheet = new Sheet();
		sheet.add(0, new LongAttribute(count));
		controls.addItem(sheet, row);
	}

	private void insert(String sql) {
		InsertParser parser = new InsertParser();
		Insert insert = parser.splitInsert(sql, TouchPool.getInstance());
		
		SiteHost top = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();

		// clear items
		controls.clearItems();

		SQLCaller caller = new SQLCaller();
		int stamp = caller.insert(top, local, insert);

		if(stamp > 0) {
			controls.showMessage("insert %d item", stamp);
		} else {
			controls.showFault("insert failed");
		}
	}

	private void inject(String sql) {
		InsertParser parser = new InsertParser();
		Inject inject = parser.splitInject(sql, TouchPool.getInstance());
		
		SiteHost top = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();

		// clear items
		controls.clearItems();

		SQLCaller caller = new SQLCaller();
		int stamp = caller.inject(top, local, inject);

		if(stamp > 0) {
			controls.showMessage("inject %d item", stamp);
		} else {
			controls.showFault("inject failed");
		}
	}
	
	private void update(String sql) {
		UpdateParser parser = new UpdateParser();
		Update update = parser.split(sql, TouchPool.getInstance());
		
		SiteHost top = TouchPool.getInstance().getRemote();
		LiveSite local = Launcher.getInstance().getLocal();
		
		// show update count
		SQLCaller caller = new SQLCaller();
		long count = caller.update(top, local, update);
		// update head
		String[] head = { "count" };
		controls.updateTable(head);
		// show result
		com.lexst.sql.column.Long value = new com.lexst.sql.column.Long((short) 1, count);
		Row row = new Row();
		row.add(value);
		
		Sheet sheet = new Sheet();
		sheet.add(0, new LongAttribute("count"));
		controls.addItem(sheet, row);
	}
	
	private void showMessage(Space space, Address[] hosts) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("accpeted '%s'", space));
		for (int i = 0; hosts != null && i < hosts.length; i++) {
			sb.append("\n");
			sb.append(String.format("site: %s", hosts[i].toString()));
		}
		controls.showMessage(sb.toString());
	}
	
	private void showMessage(String naming, Address[] hosts) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("accpeted '%s'", naming));
		for (int i = 0; hosts != null && i < hosts.length; i++) {
			sb.append("\n");
			sb.append(String.format("site: %s", hosts[i].toString()));
		}
		controls.showMessage(sb.toString());
	}

	/**
	 * 启动重构
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
			controls.showFault("cannot accepted rebuild '%s'", host.getSpace());
			Logger.error(exp);
		}
	}
	
	private void loadIndex(String sql) {
		IndexParser parser = new IndexParser();
		RebuildHostResult host = parser.splitLoadIndex(sql, true, TouchPool.getInstance());
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().loadIndex(null, local, host.getSpace(), host.getAddresses());
			this.showMessage(host.getSpace(), s);
		} catch (VisitException exp) {
			controls.showFault("cannot accpeted index '%s'", host.getSpace());
			Logger.error(exp);
		}
	}

	private void stopIndex(String sql) {
		IndexParser parser = new IndexParser();
		RebuildHostResult host = parser.splitStopIndex(sql, true, TouchPool.getInstance());
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().stopIndex(null, local, host.getSpace(), host.getAddresses());
			this.showMessage(host.getSpace(), s);
		} catch (VisitException exp) {
			controls.showFault("cannot accpeted index '%s'", host.getSpace());
			Logger.error(exp);
		}
	}

	private void loadChunk(String sql) {
		ChunkParser parser = new ChunkParser();
		RebuildHostResult sh = parser.splitLoadChunk(sql, true, TouchPool.getInstance());
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().loadChunk(null, local, sh.getSpace(), sh.getAddresses());
			this.showMessage(sh.getSpace(), s);
		} catch (VisitException exp) {
			controls.showFault("cannot accpeted index '%s'", sh.getSpace());
			Logger.error(exp);
		}
	}
	
	private void stopChunk(String sql) {
		ChunkParser parser = new ChunkParser();
		RebuildHostResult sh = parser.splitStopChunk(sql, true, TouchPool.getInstance());
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().stopChunk(null, local, sh.getSpace(), sh.getAddresses());
			this.showMessage(sh.getSpace(), s);
		} catch (VisitException exp) {
			controls.showFault("cannot accpeted chunk '%s'", sh.getSpace());
			Logger.error(exp);
		}
	}
	
	private void buildTask(String sql) {
		BuildTaskParser parser = new BuildTaskParser();
		NamingHostResult sh = parser.split(sql);
		LiveSite local = Launcher.getInstance().getLocal();
		try {
			Address[] s = TouchPool.getInstance().buildTask(null, local, sh.getNaming(), sh.getAddresses());
			this.showMessage(sh.getNaming(), s);
		} catch (VisitException exp) {
			controls.showFault("cannot accpeted naming task '%s'", sh.getNaming());
			Logger.error(exp);
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
			controls.showFault("show chunksize, failed!");
			Logger.error(exp);
			return;
		}
		
		String s = LiveUtil.format_size(chunksize);

		Table head = new Table();
		head.add(new com.lexst.sql.column.attribute.CharAttribute((short)1, "chunk-size"));
		controls.updateTable(head);

		Row row = new Row();
		row.add(new com.lexst.sql.column.Char((short)1, s.getBytes()));
		controls.addItem(head, row);
	}

	private void showSite(String sql) {
		LiveSite local = Launcher.getInstance().getLocal();

		ShowSiteParser parser = new ShowSiteParser();
		ShowSiteResult result = parser.split(sql);

		SiteHost[] sites = null;
		try {
			sites = TouchPool.getInstance().showSite(null, local, result.getSiteFimaly(), result.getAddresses());
		} catch (VisitException exp) {
			controls.showFault("visit error");
			Logger.error(exp);
			return;
		}

		if (sites != null && sites.length > 0) {
			Table head = new Table();
			head.add(new com.lexst.sql.column.attribute.CharAttribute((short) 1, "IP"));
			head.add(new com.lexst.sql.column.attribute.IntegerAttribute((short) 2, "TCP PORT", 0));
			head.add(new com.lexst.sql.column.attribute.IntegerAttribute((short) 3, "UDP PORT", 0));
			controls.updateTable(head);

			for (SiteHost site : sites) {
				Row row = new Row();
				byte[] s = new UTF8().encode(site.getAddress().getSpecification());
				row.add(new com.lexst.sql.column.Char((short) 1,  s));
				row.add(new com.lexst.sql.column.Integer((short) 2, site.getTCPort()));
				row.add(new com.lexst.sql.column.Integer((short) 3, site.getUDPort()));
				controls.addItem(head, row);
			}
		} else {
			controls.showFault("cannot find address!");
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
		controls.showMessage("load collect resource from %s, item count:%d\r\n%s", path, items, buff.toString());
	}
	
	private void testCollectTask(String sql) {
		CollectParser parser = new CollectParser();
		String naming = parser.splitCollectTask(sql);
		// find task
		CollectTask task = CollectTaskPool.getInstance().find(naming);
		// show result
		if(task == null) {
			controls.showFault("cannot find '%s'!", naming);
		} else {
			controls.showMessage("'%s' existed!", naming);
		}
		
//		// test code, begin
////		String class_name = "org.lexst.collect.Block";
//		String class_name = "org.lexst.collect.Block";
//		Class<?> cls = CollectPool.getInstance().findClass(class_name);
//		if(cls == null) {
//			Logger.error("cannot find '%s' class", class_name);
//		} else {
//			Logger.info("find '%s' class", class_name);
//		}
//		
//		try {
////			cls = ClassLoader.getSystemClassLoader().loadClass(class_name);
////			cls = Class.forName(class_name, true, ClassLoader.getSystemClassLoader());
//			cls = Class.forName(class_name, true, CollectPool.getInstance().getClassLoader());
//			if (cls == null) {
//				Logger.error("cannot load find '%s' class", class_name);
//			} else {
//				Logger.info("loader '%s' class", class_name);
//			}
//		} catch (ClassNotFoundException exp) {
//			Logger.error(exp);
//		}
//		// test code, end
	}
	
	private void showTask(String sql) {
		ShowTaskParser parser = new ShowTaskParser();
		TaskHostResult host = parser.splitShowTask(sql);
		String tag = host.getTag(); //keyword: diffuse,aggregate,collect,build,all
		
		List<TaskAddress> list = new ArrayList<TaskAddress>();
		if ("collect".equalsIgnoreCase(tag) || "all".equalsIgnoreCase(tag)) {
			Set<Naming> s = CollectTaskPool.getInstance().getNamings();
			for (Naming naming : s) {
				list.add(new TaskAddress(naming.toString(), "localhost"));
			}
		}
		if (!"collect".equalsIgnoreCase(tag) || "all".equalsIgnoreCase(tag)) {
			LiveSite local = Launcher.getInstance().getLocal();
			
			try {
				List<TaskAddress> s = TouchPool.getInstance().showTask(null, local.getHost(), tag, host.getAddresses());
				if(s != null) list.addAll(s);
			} catch (VisitException exp) {
				controls.showFault("find task naming, failed!");
				return;
			}
		}

		// sort
		if (!list.isEmpty()) {
			java.util.Collections.sort(list);
		}
		
		Table head = new Table();
		head.add(new CharAttribute((short)1, "naming"));
		head.add(new CharAttribute((short)2, "address"));
		controls.updateTable(head);
		
		for(TaskAddress naming: list) {
			Row row = new Row();
			byte[] b = naming.getNaming().getBytes();
			byte[] addr = naming.getAddress().getBytes();
			row.add(new com.lexst.sql.column.Char((short)1, b));
			row.add(new com.lexst.sql.column.Char((short)2, addr));
			controls.addItem(head, row);
		}
	}

	
	public void execute() {
		String sql = txtSQL.getText().trim();
		if(sql.length() == 0) return;
		
		// 打印帮助信息
		if("HELP".equalsIgnoreCase(sql)) {
			LiveStreamInvoker invoker = new LiveStreamInvoker();
			String s = invoker.help();
			this.controls.showMessage(s);
			return;
		}
		
		// 执行命令
		boolean success = false;
		try {			
			success = checker.isCreateSchema(sql, TouchPool.getInstance());
			if (success) createSchema(sql);
			if (!success) {
				success = checker.isDropSchema(sql, TouchPool.getInstance());
				if (success) dropSchema(sql);
			}
			if (!success) {
				success = checker.isShowSchema(sql, TouchPool.getInstance());
				if (success) showSchema(sql);
			}
			if(!success) {
				success = checker.isCreateUser(sql, TouchPool.getInstance());
				if(success) createUser(sql);
			}
			if(!success) {
				success = checker.isDropUser(sql, TouchPool.getInstance());
				if(success) dropUser(sql);
			}
			if(!success) {
				success = checker.isDropSHA1User(sql);
				if(success) dropSHA1User(sql);
			}
			if(!success) {
				success = checker.isAlterUser(sql, TouchPool.getInstance());
				if(success) alterUser(sql);
			}
			if(!success) {
				success = checker.isGrant(sql, TouchPool.getInstance());
				if(success) grant(sql);
			}
			if(!success) {
				success = checker.isRevoke(sql, TouchPool.getInstance());
				if(success) revoke(sql);
			}
			if(!success) {
				success = checker.isSetChunkSize(sql, TouchPool.getInstance());
				if (success) setChunkSize(sql);
			}
			if(!success) {
				success = checker.isSwitchTime(sql, TouchPool.getInstance());
				if(success) setSwitchTime(sql);
			}
			
			if(!success) {
				success = checker.isCreateTable(sql, TouchPool.getInstance());
				if(success) createTable(sql);
			}
			if (!success) {
				success = checker.isDropTable(sql, TouchPool.getInstance());
				if (success) deleteTable(sql);
			}
			if (!success) {
				success = checker.isShowTable(sql, TouchPool.getInstance());
				if (success) showTable(sql);
			}
			
			if(!success) {
				success = checker.isSelect(sql, TouchPool.getInstance());
				if(success) select(sql);
			}
			if(!success) {
				success = checker.isDelete(sql, TouchPool.getInstance());
				if(success) delete(sql);
			}
			if (!success) {
				success = checker.isInsert(sql, TouchPool.getInstance());
				if (success) insert(sql);
			}
			if (!success) {
				success = checker.isInject(sql, TouchPool.getInstance());
				if (success) inject(sql);
			}
			if (!success) {
				success = checker.isUpdate(sql, TouchPool.getInstance());
				if (success) update(sql);
			}
			// 分布计算
			if (!success) {
				success = checker.isConduct(sql, TouchPool.getInstance());
				if (success) conduct(sql);
			}
			if (!success) {
				success = checker.isLoadIndex(sql, TouchPool.getInstance());
				if (success) loadIndex(sql);
			}
			if (!success) {
				success = checker.isStopIndex(sql, TouchPool.getInstance());
				if (success) stopIndex(sql);
			}
			if (!success) {
				success = checker.isLoadChunk(sql, TouchPool.getInstance());
				if (success) loadChunk(sql);
			}
			if (!success) {
				success = checker.isStopChunk(sql, TouchPool.getInstance());
				if (success) stopChunk(sql);
			}
			if(!success) {
				success = checker.isRebuild(sql, TouchPool.getInstance());
				if(success) loadRebuild(sql);
			}
			if(!success) {
				success = checker.isBuildTask(sql);
				if(success) buildTask(sql);
			}
			if (!success) {
				success = checker.isShowChunkSize(sql, TouchPool.getInstance());
				if (success) showChunksize(sql);
			}
			if (!success) {
				success = checker.isShowSite(sql);
				if (success) showSite(sql);
			}
			if (!success) {
				success = checker.isSetCollectPath(sql);
				if (success) setCollectPath(sql);
			}
			if (!success) {
				success = checker.isTestCollectPath(sql);
				if (success) testCollectTask(sql);
			}
			if(!success) {
				success = checker.isShowTask(sql);
				if(success) showTask(sql);
			}
		} catch (SQLSyntaxException exp) {
			String msg = exp.getMessage();
			controls.showFault(msg);
		}

		if(!success) {
			controls.showFault("invalid sql syntax");
		}
	}

	private void initToolBar() {
		JButton[] cmds = { cmdCheck, cmdGo, cmdFont, cmdCut, cmdClear };
		String[] images = { "sqlcheck_16.png", "go_16.png", "font_16.png", "cut_16.png", "clear_16.png" };
		String[] tips = {"Check SQL Syntax (F5)", "Run (F6)", "Set Text Font", "Clear Message", "Clear Table Information"};
		ResourceLoader loader = new ResourceLoader("conf/terminal/image/window/query/");
		for (int i = 0; i < cmds.length; i++) {
			Icon icon = loader.findImage(images[i]);
			cmds[i].setIcon(icon);
			cmds[i].addActionListener(this);
			cmds[i].setToolTipText(tips[i]);
			toolbar.add(cmds[i]);
		}
		toolbar.setFloatable(false);
		toolbar.setOrientation(JToolBar.VERTICAL);
	}

	public void init() {
		this.initToolBar();

		JPanel top = initSQLPane();
		controls.init();
		JSplitPane pane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, top, controls);
		
        pane.setContinuousLayout(true);
        pane.setOneTouchExpandable(true);
        pane.setResizeWeight(0.05);
        pane.setBorder(new EmptyBorder(0, 0, 0, 0));

        setLayout(new BorderLayout());
		add(pane, BorderLayout.CENTER);
	}

	
	private JPanel initSQLPane() {
		String html = "<html><body>Command Terminal&nbsp;&nbsp;(ALT+S)<br>SHIFT+CTRL+&lt; | SHIFT+CTRL+&gt; </body></html>";
		Font font = txtSQL.getFont();
		font = new Font(font.getName(), font.getStyle(), font.getSize() + 4);
		txtSQL.setFont(font);
		txtSQL.setPreferredSize(new Dimension(10, 80));
		txtSQL.addKeyListener(new SQLKeyAdapter());
		txtSQL.setToolTipText(html);
		txtSQL.setFocusAccelerator('S');
		txtSQL.setBorder(new EmptyBorder(2, 2, 2, 2));

		JScrollPane top = new JScrollPane(txtSQL);

		JPanel panel = new JPanel();
		panel.setLayout(new BorderLayout());
		panel.add(top, BorderLayout.CENTER);
		panel.add(toolbar, BorderLayout.EAST);
		return panel;
	}

}