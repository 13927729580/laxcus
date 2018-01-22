/**
 *
 */
package com.lexst.work;

import java.io.*;
import java.util.*;

import org.w3c.dom.*;

import com.lexst.algorithm.*;
import com.lexst.algorithm.disk.*;
import com.lexst.algorithm.to.*;
import com.lexst.log.client.*;
import com.lexst.pool.site.*;
import com.lexst.remote.client.home.*;
import com.lexst.site.work.*;
import com.lexst.sql.schema.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;
import com.lexst.visit.*;
import com.lexst.visit.impl.work.*;
import com.lexst.work.pool.*;
import com.lexst.xml.*;

/**
 * WORK节点启动引导入口
 * 
 *
 */
public class Launcher extends JobLauncher implements TaskEventListener, FromListener { 

	private static Launcher selfHandle = new Launcher();
	
	/** 当前WORK节点地址配置 */
	private WorkSite local = new WorkSite();
	
	/**
	 * default constructor
	 */
	private Launcher() {
		super();
		super.setExitVM(true);
		streamImpl = new WorkStreamInvoker();
		packetImpl = new WorkPacketInvoker(fixpPacket);
	}

	/**
	 * @return
	 */
	public static Launcher getInstance() {
		return Launcher.selfHandle;
	}
	
	/**
	 * get local
	 * @return
	 */
	public WorkSite getLocal() {
		return this.local;
	}
	
	/*
	 * 接受ToTaskPool通知，任务命名集合已改变，更新节点上的命名记录。
	 * @see com.lexst.algorithm.TaskEventListener#updateNaming()
	 */
	@Override
	public void updateNaming() {
		// 通知线程，重新注册到HOME节点
		setOperate(BasicLauncher.RELOGIN);
	}
	
	/*
	 * FromPool通知，DATA宿主节点已经更新(CALL节点有效，WORK节点不处理)
	 * @see com.lexst.pool.site.FromListener#updateDataRecord()
	 */
	@Override
	public void updateDataRecord() {
		
	}
	
	/**
	 * 客户端RPC调用，保持连接状态
	 */
	public void nothing() {
		
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// 连接HOME节点
		HomeClient client = bring();
		if (client == null) {
			Logger.error("Launcher.init, cannot find home site %s", getHubSite());
			return false;
		}

		//1. 启动日志服务(定期将本地日志传入日志服务器)
		boolean	success = loadLog(local.getFamily(), client);
		Logger.note("Launcher.init, load log", success);
		if(!success) return false;
		//2. 设置WORK节点超时时间
		if(success) {
			success = loadTimeout(local.getFamily(), client);
			Logger.note(success, "Launcher.init, set site timeout %d", getSiteTimeout());
			if(!success) stopLog();
		}
		//3. 设置系统时间(以HOME节点时间为准建立统一时间轴，HOME节点又是以TOP节点为准)
		if (success) {
			super.loadTime(client);
		}
		//4. 加载本地数据库表资源
		if(success) {
			success = loadTable(client);
			Logger.note("Launcher.init, load table", success);
			if (!success) stopLog();
		}
		//5. 启动FIXP监听服务器
		if (success) {
			Class<?>[] clazz = { WorkVisitImpl.class };
			success = loadListen(clazz, local.getHost());
			Logger.note("Launcher.init, load listen", success);
			if (!success) stopLog();
		}
		//6. 启动WORK节点的所有资源管理池
		if(success) {
			success = loadPool();
			Logger.note("Launcher.init, load pool", success);
			if (!success) {
				stopListen();
				stopLog();
			}
		}
		//7. 注册本节点到HOME服务器
		if (success) {
			success = login(client);
			Logger.note("Launcher.init, login", success);
			if (!success) {
				stopPool();
				stopListen();
				stopLog();
			}
		}
	
		// 关闭与HOME节点的连接
		complete(client);
		
		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("Launcher.process, into ...");
		this.refreshEndTime();
		
		while (!isInterrupted()) {
			long end = System.currentTimeMillis() + 1000;

			if (super.isLoginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.login(null);
			} else if(super.isReloginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.relogin(null);
			} else if (isMaxSiteTimeout()) {
				this.refreshEndTime();
				this.relogin(null);
			} else if (isSiteTimeout()) {
				hello(local.getFamily(), getHubSite()); // active to home
			}

			long timeout = end - System.currentTimeMillis();
			if (timeout > 0) this.delay(timeout);
		}
		Logger.info("Launcher.process, exit");
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// 从HOME服务器注销
		logout(null);
		// 停止资源配置池
		stopPool();
		// 关闭监听服务
		stopListen();
		// 停止日志服务
		stopLog();
	}

	/**
	 * 从aggregate任务存储池中取数据库表名，向HOME节点请求数据库表
	 * 
	 * @param client
	 * @return
	 */
	private boolean loadTable(HomeClient client) {
		Set<Space> array = new TreeSet<Space>();
		for (Naming naming : ToTaskPool.getInstance().getNamings()) {
			Project project = ToTaskPool.getInstance().findProject(naming);
			array.addAll(project.getSpaces());
		}
		if (array.isEmpty()) return true;
		
		boolean success = false;
		boolean nullable = (client == null);
		try {
			if (nullable) client = super.bring();
			if (client == null) return false;

			for (Space space : array) {
				Table table = client.findTable(space);
				if (table == null) {
					Logger.error("Launcher.loadTable, cannot find %s", space);
					return false;
				}
				FromPool.getInstance().addTable(table);
				Logger.info("Launcher.loadTable, load table '%s'", space);
			}

			success = true;
		} catch (VisitException e) {
			Logger.error(e);
		} catch (Throwable e) {
			Logger.fatal(e);
		} finally {
			if (nullable) complete(client);
		}
		return success;
	}

	/**
	 * load work pool
	 * @return
	 */
	private boolean loadPool() {
		Logger.info("Launcher.loadPool, load work pool");

		// 设置FROM数据池配置参数
		FromPool.getInstance().setHome(super.getHubSite());
		FromPool.getInstance().setFromListener(this);

		// aggregate任务池设置任务事件监听接口
		ToTaskPool.getInstance().setTaskEventListener(this);
		// 给aggregate任务池设置diffuse数据选择器
		ToTaskPool.getInstance().setFromChooser(FromPool.getInstance());
		// 设置本地绑定地址副本
		DiskPool.getInstance().setLocal(local.getHost());

		// 启动磁盘数据管理池
		boolean success = DiskPool.getInstance().start();
		// 启动"aggregate"包配置监视器
		if (success) {
			success = ToTaskPool.getInstance().start();
		}
		// 启动DATA节点记录配置池
		if(success) {
			success = FromPool.getInstance().start();
		}
		// 启动分布计算conduct工作池
		if (success) {
			success = ConductPool.getInstance().start();
		}
		return success;
	}

	/**
	 * stop work pool
	 */
	private void stopPool() {
		Logger.info("Launcher.stopPool, stop all pool...");

		ConductPool.getInstance().stop();
		FromPool.getInstance().stop();
		ToTaskPool.getInstance().stop();
		DiskPool.getInstance().stop();

		while (ConductPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (FromPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (ToTaskPool.getInstance().isRunning()) {
			this.delay(200);
		}
		while (DiskPool.getInstance().isRunning()) {
			this.delay(200);
		}
	}

	/**
	 * login to home site
	 * @param client
	 * @return
	 */
	private boolean login(HomeClient client) {
		Logger.info("Launcher.login, %s login to %s", local.getHost(), getHubSite());
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) return false;
		
		// save all naming
		local.clear();
		local.addAll(ToTaskPool.getInstance().getNamings());

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.login(local);
				break;
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(1000);
		}
		if (nullable) complete(client);
		return success;
	}
	
	/**
	 * logout from home site
	 * @param client
	 */
	private boolean logout(HomeClient client) {
		Logger.info("Launcher.logout, %s logout from %s", local.getHost(), getHubSite());
		
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
			this.delay(1000);
		}
		if (nullable) complete(client);
		return success;
	}
	
	/**
	 * relogin site
	 * @param client
	 * @return
	 */
	private boolean relogin(HomeClient client) {
		Logger.info("Launcher.relogin, %s from %s", local.getHost(), getHubSite());
			
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) return false;
		
		// 更新全部命令对象
		local.update(ToTaskPool.getInstance().getNamings());

		// 注册到HOME节点
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
			this.delay(1000);
		}
		if (nullable) complete(client);
		return success;
	}


	/**
	 * 加载并且解析WORK节点配置文件
	 * 
	 * @param filename
	 * @return
	 */
	private boolean loadLocal(String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) {
			return false;
		}
		// 解析并且设置HOME节点
		SiteHost host = super.splitHome(document);
		if(host == null) return false;
		super.setHubSite(host);
		
		// 解析本地WORK节点绑定地址
		host = super.splitLocal(document);
		if (host == null)return false;
		local.setHost(host);
		
		// 解析远程关闭配置(接受命令的节点地址)
		if (!loadShutdown(document)) {
			Logger.error("Launcher.loadLocal, cannot resolve shutdown address");
			return false;
		}
		
		// 解析安全管理配置
		if(!super.loadSecurity(document)) {
			Logger.error("Launcher.loadLocal, cannot parse security file");
			return false;
		}

		// aggregate热发布任务管理目录
		String path = xml.getXMLValue(document.getElementsByTagName("aggregate-root"));
		if (path == null || path.trim().length() == 0) {
			return false;
		}
		ToTaskPool.getInstance().setRoot(path.trim());

		// conduct数据存储目录
		path = xml.getXMLValue(document.getElementsByTagName("conduct-directory"));
		if (path == null || path.trim().length() == 0) {
			return false;
		}
		DiskPool.getInstance().setRoot(path.trim());
		
		// 加载目录配置
		return Logger.loadXML(filename);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args == null || args.length < 1) {
			Logger.error("parameters missing!");
			Logger.gushing();
			return;
		}
		// 加载配置文件
		String filename = args[0];
		boolean success = Launcher.getInstance().loadLocal(filename);
		Logger.note("Launcher.main, load local", success);
		// start thread
		if (success) {
			success = Launcher.getInstance().start();
			Logger.note("Launcher.main, start service", success);
		}
		if (!success) {
			Logger.gushing();
		}
	}

}