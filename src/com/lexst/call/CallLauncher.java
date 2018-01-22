/**
 *
 */
package com.lexst.call;

import java.io.*;
import java.util.*;

import org.w3c.dom.*;

import com.lexst.algorithm.*;
import com.lexst.algorithm.balance.*;
import com.lexst.algorithm.init.*;
import com.lexst.call.pool.*;
import com.lexst.log.client.*;
import com.lexst.pool.site.*;
import com.lexst.remote.client.home.*;
import com.lexst.site.call.*;
import com.lexst.sql.schema.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;
import com.lexst.visit.*;
import com.lexst.visit.impl.call.*;
import com.lexst.xml.*;

/**
 * CALL节点启动器。<br>
 *
 */
public class CallLauncher extends JobLauncher implements TaskEventListener, FromListener, ToListener {
	
	/** 当前CALL节点配置 */
	private CallSite local = new CallSite();

	/**
	 * default constructor
	 */
	protected CallLauncher() {
		super();
		super.setLogging(true);
		packetImpl = new CallPacketInvoker(this, fixpPacket);
		streamImpl = new CallStreamInvoker();
		CallVisitImpl.setInstance(this);
	}

	/**
	 * call site configure
	 * @return
	 */
	public CallSite getLocal() {
		return local;
	}

	/*
	 * InitTaskPool或者BalanceTaskPool通知，命名集合已经改变，更新全部本地配置
	 * @see com.lexst.algorithm.TaskEventListener#updateNaming()
	 */
	@Override
	public void updateNaming() {
		Set<Naming> set = InitTaskPool.getInstance().getNamings();
		local.updateInitials(set);
		set = BalanceTaskPool.getInstance().getNamings();
		local.updateBalances(set);

		// 重新注册到HOME节点
		setOperate(BasicLauncher.RELOGIN);
	}
	
	/*
	 * FromPool通知，DATA节点上的配置已经改变，更新CallSite与DATA节点有关的记录
	 * @see com.lexst.pool.site.FromListener#updateDataRecord()
	 */
	@Override
	public void updateDataRecord() {
		// 命新diffuse命名
		List<Naming> list = FromPool.getInstance().getNamings();
		local.updateFroms(list);
		// 更新数据库表记录
		List<Space> spaces = FromPool.getInstance().getSpaces();
		local.updateSpaces(spaces);

		// 重新注册到HOME节点
		setOperate(BasicLauncher.RELOGIN);
	}

	/*
	 * ToPool通知，WORK节点上的配置已经改变，更新CallSite与WORK节点有关的记录
	 * @see com.lexst.pool.site.ToListener#updateWorkRecord()
	 */
	@Override
	public void updateWorkRecord() {
		// 更新aggregate命名
		List<Naming> list = ToPool.getInstance().getNamings();
		local.updateTos(list);
		// 重新注册到HOME服务器
		setOperate(BasicLauncher.RELOGIN);
	}

	/**
	 * ping service
	 */
	public void nothing() {
		// Logger.info("this is nothing method");
	}

	/**
	 * 启动数据库表关联服务
	 * @param table
	 * @return
	 */
	public boolean createSpace(Table table) {
		Space space = table.getSpace();
		Table object = FromPool.getInstance().findTable(space);
		if (object != null) {
			Logger.error("CallLauncher.createSpace, existed space '%s'", space);
			return false;
		}
		Logger.info("CallLauncher.createSpace, create table space '%s'", space);
		// 保存数据库表配置
		FromPool.getInstance().addTable(table);
		local.addSpace(space);
		this.setOperate(BasicLauncher.RELOGIN);
		return true;
	}

	/**
	 * 停止数据库表服务
	 * @param space
	 * @return
	 */
	public boolean deleteSpace(Space space) {
		Table table = FromPool.getInstance().findTable(space);
		if (table == null) {
			Logger.error("CallLauncher.deleteSpace, cannot found space '%s'", space);
			return false;
		}

		// 删除内存中保留了数据库表配置
		FromPool.getInstance().removeTable(space);
		CodePointCollector.getInstance().removeTable(space);
		local.removeSpace(space);

		// 重新注册
		this.setOperate(BasicLauncher.RELOGIN);
		return true;
	}
	
//	/**
//	 * 启动时，因为数据库表集合是空，需要HOME节点请求分配一部分数据库表空间来完成工作
//	 * @param client
//	 * @param num
//	 * @return
//	 */
//	private boolean balance(HomeClient client, int num) {
//		// 如果有分配,就不请求新的空间
//		if (loader.size() > 0) return true;
//		
//		// 如果没有,请求新的空间定义
//		boolean nullable = (client == null);
//		if(nullable) client = bring(homehost);
//		if(client == null) return false;
//
//		// 向HOME节点申请分配数据库表名称
//		try {
//			Space[] s = client.balance(num);
//			for (int i = 0; s != null && i < s.length; i++) {
//				Logger.info("CallLauncher.balance, space '%s'", s[i]);
//				loader.add(s[i]);
//			}
//		} catch (VisitException exp) {
//			Logger.error(exp);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		}
//		if(nullable) complete(client);
//
//		return true;
//	}

	/**
	 * load database space
	 *
	 * @param client
	 * @return
	 */
	private boolean loadTable(HomeClient client) {
		// 保存从HOME节点分配的数据库表名
		Set<Space> array = new TreeSet<Space>();
		// 保存从热发布任务管理中的数据库表名
		for (Naming naming : InitTaskPool.getInstance().getNamings()) {
			Project project = InitTaskPool.getInstance().findProject(naming);
			array.addAll(project.getSpaces());
		}
		for (Naming naming : BalanceTaskPool.getInstance().getNamings()) {
			Project project = BalanceTaskPool.getInstance().findProject(naming);
			array.addAll(project.getSpaces());
		}
		
		// 因为启动时FromPool数据库表集合是空，需要HOME节点请求分配一部分数据库表空间来完成工作
		// 向HOME节点请求20个数据库表(数量需要再酌情考虑到!!!)
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) return false;

		try {
			Space[] spaces = client.balance(20);
			for (int i = 0; spaces != null && i < spaces.length; i++) {
				Logger.info("CallLauncher.balance, space '%s'", spaces[i]);
				array.add( (Space)spaces[i].clone() );
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}

		// 向HOME节点查找数据库表资源，保存到FromPool
		for (Space space : array) {
			for (int i = 0; i < 3; i++) {
				try {
					if(client.isClosed()) client.reconnect();
					Table table = client.findTable(space);
					if (table == null) {
						Logger.error("CallLauncher.loadTable, cannot find table '%s'", space);
					} else {
						Logger.info("CallLauncher.loadTable, load table '%s'", space);
						FromPool.getInstance().addTable(table);
						local.addSpace(space);
					}
					break;
				} catch (IOException exp) {
					Logger.error(exp);
				} catch (Throwable exp) {
					Logger.fatal(exp);
				}
				client.close();
				this.delay(1000);
			}
		}
		if(nullable) complete(client);
		return true;
	}

	/**
	 * login to hoem site
	 * @param client
	 * @return
	 */
	private boolean login(HomeClient client) {
		Logger.info("CallLauncher.login, %s to %s", local.getHost(), getHubSite());
		
		// 更新全部记录
		this.updateNaming();
		this.updateDataRecord();
		this.updateWorkRecord();
		
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) {
			Logger.error("CallLauncher.login, cannot connect %s", getHubSite());
			return false;
		}

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.login(local);
				break;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			// when error, close
			client.close();
			delay(5000);
		}
		if(nullable) complete(client);
		return success;
	}
	
	/**
	 * relogin to home site
	 * @param client
	 * @return
	 */
	private boolean relogin(HomeClient client) {
		Logger.info("CallLauncher.relogin, %s to %s", local.getHost(), getHubSite());

		boolean nullable = (client == null);
		if(nullable) client = bring();
		if(client == null) {
			Logger.error("CallLauncher.relogin, cannot connect %s", getHubSite());
			return false;
		}

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.relogin(local);
				break;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(5000);
		}
		if(nullable) complete(client);
		return success;
	}

	/**
	 * logout from home site
	 */
	private boolean logout(HomeClient client) {
		Logger.info("CallLauncher.logout, %s from %s", local.getHost(), getHubSite());
		
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) return false;

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.logout(local.getFamily(), local.getHost());
				break;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(5000);
		}
		if(nullable) complete(client);
		return success;
	}

	/**
	 * start call monitor
	 * @return
	 */
	private boolean loadPool() {
		// HOME地址
		BuildPool.getInstance().setHome(getHubSite());
		LivePool.getInstance().setHome(getHubSite());

		// 代码位收集器(从INJECT/INSERT提取字符集代码位，发送给HOME节点)
		CodePointCollector.getInstance().setHome(getHubSite());
//		CodePointCollector.getInstance().setLauncher(this);
		
		// 命名任务池
		FromPool.getInstance().setHome(getHubSite());
		FromPool.getInstance().setFromListener(this);
		ToPool.getInstance().setHome(getHubSite());
		ToPool.getInstance().setToListener(this);

		// 热发布初始化任务池
		InitTaskPool.getInstance().setTaskEventListener(this);
		InitTaskPool.getInstance().setFromChooser(FromPool.getInstance());
		InitTaskPool.getInstance().setToChooser(ToPool.getInstance());

		// 热发布平衡任务池
		BalanceTaskPool.getInstance().setTaskEventListener(this);

		// 启动
		boolean success = DataPool.getInstance().start();
		if (success) {
			success = WorkPool.getInstance().start();
		}
		if (success) {
			success = LivePool.getInstance().start();
		}
		if (success) {
			success = BuildPool.getInstance().start();
		}
		if (success) {
			success = CodePointCollector.getInstance().start();
		}
		
		// 启动aggregate任务资源池(只限命名)
		if(success) {
			success = ToPool.getInstance().start();
		}
		// 启动diffuse任务资源池(diffuse命名和数据库表所有资源)
		if(success) {
			success = FromPool.getInstance().start();
		}
		// 启动平衡计算命名任务配置池
		if (success) {
			success = BalanceTaskPool.getInstance().start();
		}
		// 启动初始化命名任务配置池
		if (success) {
			success = InitTaskPool.getInstance().start();
		}
		
		return success;
	}

	/**
	 * stop call monitor
	 */
	private void stopPool() {
		BuildPool.getInstance().stop();
		LivePool.getInstance().stop();
		DataPool.getInstance().stop();
		WorkPool.getInstance().stop();
		
		CodePointCollector.getInstance().stop();
//		CodePointPool.getInstance().stop();

		InitTaskPool.getInstance().stop();
		BalanceTaskPool.getInstance().stop();

		FromPool.getInstance().stop();
		ToPool.getInstance().stop();

		while(BuildPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while(LivePool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (DataPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (WorkPool.getInstance().isRunning()) {
			this.delay(200);
		}
		
		while (CodePointCollector.getInstance().isRunning()) {
			this.delay(200);
		}
		
		while (InitTaskPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (BalanceTaskPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (FromPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (ToPool.getInstance().isRunning()) {
			this.delay(200);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		HomeClient client = bring();
		if (client == null) return false;

		//1. 启动日志服务
		boolean success = loadLog(local.getFamily(), client);
		Logger.note("CallLauncher.init, load log", success);
		//2. 从HOME节点确定CALL节点超时时间(在超时前发送心跳包保持活跃状态)
		if (success) {
			success = loadTimeout(local.getFamily(), client);
			Logger.note(success, "CallLauncher.init, set site timeout %d", getSiteTimeout());
			if (!success) stopLog();
		}
		//3. 启动FIXP监视器
		if (success) {
			Class<?>[] cls = { CallVisitImpl.class };
			success = loadListen(cls, local.getHost());
			Logger.note("CallLauncher.init, load listen", success);
			if (!success) stopLog();
		}
		
//		//4. apply space from home site
//		if (success) {
//			success = balance(client, 10);
//			Logger.note(success, "CallLauncher.init, apply space, size:%d", loader.size());
//			if (!success) {
//				stopListen();
//				stopLog();
//			}
//		}
		
		//4. 启动资源配置池
		if (success) {
			success = loadPool();
			Logger.note("CallLauncher.init, load pool", success);
			if (!success) {
				stopListen();
				stopLog();
			}
		}
		//5. 加载数据库表
		if (success) {
			success = loadTable(client);
			Logger.note("CallLauncher.init, load table space", success);
			if (!success) {
				stopPool();
				stopListen();
				stopLog();
			}
		}
		//6. 注册到HOME节点
		if (success) {
			success = login(client);
			Logger.note("CallLauncher.init, login", success);
			if (!success) {
				stopPool();
				stopListen();
				stopLog();
			}
		}
		complete(client);
		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		//1. 从HOME注销本地节点
		logout(null);
		//2. 停止数据池服务
		stopPool();
		//3. 停止FIXP监听服务
		stopListen();
		//4. 停止日志服务
		stopLog();
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		this.setOperate(BasicLauncher.NONE);
		Logger.info("CallLauncher.process, into...");
		this.refreshEndTime();
		
		long end, timeout;
		while (!isInterrupted()) {
			end = System.currentTimeMillis() + 1000;
			
			if (super.isLoginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.login(null);
			} else if (super.isReloginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.relogin(null);
			} else if (isMaxSiteTimeout()) {
				this.refreshEndTime();
				this.relogin(null);
			} else if (isSiteTimeout()) {
				this.hello(local.getFamily(), getHubSite());
			}
			
			timeout = end - System.currentTimeMillis();
			if (timeout > 0) delay(timeout);
		}
		Logger.info("CallLauncher.process, exit");
	}

	/**
	 * 加载/解析CALL节点资源配置
	 *
	 * @param filename
	 */
	public boolean loadLocal(String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) {
			return false;
		}

		// 解析并且设置HOME节点
		SiteHost host = splitHome(document);
		if(host == null) return false;
		super.setHubSite(host);
		
		// 解析并且保存本地节点地址
		host = splitLocal(document);
		if (host == null) return false;
		local.setHost(host);

		// 解析停止运行任务监听配置
		if(!loadShutdown(document)) {
			Logger.error("Launcher.loadLocal, cannot resolve shutdown address");
			return false;
		}
		
		// 解析FIXP安全通信配置资源
		if(!super.loadSecurity(document)) {
			Logger.error("Launcher.loadLocal, cannot resolve security file");
			return false;
		}
		
		// 解析并且设置初始化任务热发布目录
		String path = xml.getXMLValue(document.getElementsByTagName("init-task-root"));
		if (path == null || path.trim().isEmpty()) {
			return false;
		}
		InitTaskPool.getInstance().setRoot(path);

		// 解析并且设置平衡数据任务热发布目录
		path = xml.getXMLValue(document.getElementsByTagName("balance-task-root"));
		if (path == null || path.trim().isEmpty()) {
			return false;
		}
		BalanceTaskPool.getInstance().setRoot(path);
		
		// 加载日志配置
		return Logger.loadXML(filename);
	}

}