package com.lexst.log.server;

import java.io.*;
import java.net.*;
import java.util.*;

import org.w3c.dom.*;

import com.lexst.fixp.monitor.*;
import com.lexst.remote.client.home.*;
import com.lexst.site.*;
import com.lexst.site.log.*;
import com.lexst.thread.*;
import com.lexst.util.datetime.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.xml.*;

/**
 * 日志启动管理器。<br>
 * 为各类型的节点启动日志接收器，接收各类型节点的日志上传，并且写入本地磁盘。
 *
 */
public final class Launcher extends BasicLauncher {
	
	/** 日志管理器静态句柄 **/
	private static Launcher selfHandle = new Launcher();

	/** 各节点日志接收器集合  **/
	private ArrayList<FixpPacketMonitor> array = new ArrayList<FixpPacketMonitor>();

	/** HOME节点地址  **/
	private SiteHost home = new SiteHost();

	/** 本地服务器地址 **/
	private LogSite local = new LogSite();

	/**
	 * 初始化日志启动器
	 */
	private Launcher() {
		super();
		super.setExitVM(true);

		fixpStream.setPrint(false);
		fixpPacket.setPrint(false);
		packetImpl = new LogPacketInvoker(fixpPacket);
		streamImpl = new LogStreamInvoker();
	}

	/**
	 * 返回日志启动器静态句柄
	 * @return
	 */
	public static Launcher getInstance() {
		return Launcher.selfHandle;
	}

	/**
	 * 设置HOME节点
	 * 
	 * @param host
	 */
	public void setHubSite(SiteHost host) {
		this.home = new SiteHost(host);
	}

	/**
	 * 返回HOME节点
	 * 
	 * @return
	 */
	public SiteHost getHubSite() {
		return this.home;
	}

	/**
	 * 建立与HOME节点连接
	 * @return
	 */
	private LogHomeClient fetch(boolean stream) {
		SocketHost host = (stream ? home.getStreamHost() : home.getPacketHost());
		LogHomeClient client = new LogHomeClient(true, host);
		for (int i = 0; i < 3; i++) {
			try {
				client.reconnect();
				return client;
			} catch (IOException exp) {
				exp.printStackTrace();
			}
			client.close();
			this.delay(1000);
		}
		return null;
	}

	/**
	 * 关闭与HOME节点连接
	 * @param client
	 */
	private void complete(LogHomeClient client) {
		if(client == null) return;
		try {
			client.exit();
			client.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		} 
	}

	/**
	 * 从HOME节点取得本节点超时时间
	 * @param siteType
	 * @param client
	 * @return
	 */
	protected boolean loadTimeout(int siteType, LogHomeClient client) {
		boolean success = false;
		try {
			int second = client.getSiteTimeout(siteType);
			this.setSiteTimeout(second);
			System.out.printf("Launcher.loadTimeout, site timeout %d\n", second);
			success = true;
		} catch (VisitException exp) {
			exp.printStackTrace();
		} catch (Throwable exp) {
			exp.printStackTrace();
		}
		return success;
	}

	/**
	 * 注册到HOME节点
	 * @return
	 */
	private boolean login(LogHomeClient client) {
		boolean nullable = (client == null);
		if(nullable) client = fetch(true);
		if(client == null) return false;
		boolean success = false;
		try {
			success = client.login(this.local);
		} catch (VisitException exp) {
			exp.printStackTrace();
		}
		if (nullable) complete(client);
		return success;
	}

	/**
	 * 从HOME节点注销
	 * @return
	 */
	private boolean logout(LogHomeClient client) {
		boolean nullable = (client == null);
		if (nullable) client = fetch(false);
		if (client == null) return false;
		boolean success = false;
		try {
			success = client.logout(local.getFamily(), local.getHost());
		} catch (VisitException exp) {
			exp.printStackTrace();
		}
		System.out.printf("Launcher.logout, from %s %s\n", getHubSite(), (success ? "success" : "failed"));
		if (nullable) complete(client);
		return success;
	}
	
	/**
	 * 重新注册
	 * @return
	 */
	private boolean relogin(LogHomeClient client) {
		boolean nullable = (client == null);
		if (nullable) client = fetch(true);
		if (client == null) return false;
		boolean success = false;
		try {
			success = client.relogin(local);
		} catch (VisitException exp) {
			exp.printStackTrace();
		}
		if (nullable) complete(client);
		return success;
	}

	/**
	 * 启动日志服务
	 * @return
	 */
	private boolean startService() {
		// 如果是自回路或者通配符地址，必须绑定一个实际的本机内网地址
		InetAddress localIP = local.getInetAddress();
		if(localIP.isAnyLocalAddress() || localIP.isLoopbackAddress()) {
			localIP = Address.select();
		}
		local.setInetAddress(localIP);
		
		for(LogNode node : local.list()) {
			String tag = node.getTag();
			int port = node.getPort();
			
			File dir = new File(super.getResourcePath(), tag);			
			LogPacketWriter writer = new LogPacketWriter(dir);
			FixpPacketMonitor monitor = new FixpPacketMonitor(1);
			monitor.setLocal(localIP, port);
			monitor.setPacketCall(writer);
			boolean success = monitor.start();
			if(success) {
				array.add(monitor);
			}
		}
		
		return true;
	}

	/**
	 * stop log listen
	 */
	private void stopService() {
		// 关闭全部日志接收器
		for(FixpPacketMonitor monitor : array) {
			monitor.stop();
		}
	}

	/**
	 * @param client
	 */
	private boolean loadTime(LogHomeClient client) {
		boolean nullable = (client == null);
		if (nullable) client = fetch(true);
		if (client == null) return false;

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if(client.isClosed()) client.reconnect();
				long time = client.currentTime();
				System.out.printf("Launcher.loadTime, set time:%d\r\n", time);
				if (time != 0L) {
					int ret = SystemTime.set(time);
					success = (ret == 0);
					break;
				}
			} catch (VisitException exp) {
				exp.printStackTrace();
			} catch (Throwable exp) {
				exp.printStackTrace();
			}
			client.close();
			this.delay(500);
		}
		if(nullable) complete(client);
		
		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
		String time = sdf.format(new java.util.Date(System.currentTimeMillis()));
		System.out.printf("Launcher.loadTime, current time:%s %s\r\n", time, (success ? "success" : "failed"));

		return success;
	}
	
	/*
	 * 初始化服务
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		LogHomeClient client = fetch(true);
		if (client == null) {
			return false;
		}
		//1. 加载并且设置系统时间
		boolean success = loadTime(client);
		// 2. 确定日志节点超时时间
		if (success) {
			success = loadTimeout(local.getFamily(), client);
		}
		//3. 启动FIXP服务器(保持与HOME节点的通信)
		if (success) {
			success = loadListen(null, local.getHost());
		}
		//4. 启动日志服务
		if (success) {
			success = startService();
			if (!success) {
				this.stopService();
				this.stopListen();
			}
		}
		//5. 注册到HOME节点
		if (success) {
			success = login(client);
			System.out.printf("Launcher.init, login to %s %s\n", getHubSite(), (success ? "success" : "failed"));
			if (!success) {
				this.stopService();
				this.stopListen();
			}
		}
		// 关闭连接
		complete(client);
		return success;
	}

	/*
	 * release log service
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// 从HOME节点注销
		this.logout(null);
		// 停止FIXP监听
		this.stopListen();
		// 停止日志服务
		this.stopService();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		System.out.printf("Launcher.process, into...\n");
		this.refreshEndTime();
		
		while (!isInterrupted()) {
			long end = System.currentTimeMillis() + 1000;
			if (isLoginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.login(null);
			} else if (isMaxSiteTimeout()) {
				refreshEndTime();
				relogin(null);
			} else if (isSiteTimeout()) {
				hello(local.getFamily(), this.getHubSite());
			}
			
			long timeout = end - System.currentTimeMillis();
			if (timeout > 0) this.delay(timeout);
		}
	}

	/**
	 * 加载本地配置
	 * @param filename
	 * @return
	 */
	private boolean loadLocal(String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) {
			return false;
		}
		
		// 日志写入目录
		String dirname = xml.getXMLValue(document.getElementsByTagName("write-directory"));
		// 建立这个目录
		super.createResourcePath(dirname);
		
		// 解析HOME节点地址
		SiteHost host = super.splitHome(document);
		if(host == null) {
			return false;
		}
		this.setHubSite(host);
		// 解析本地主机地址
		host = super.splitLocal(document);
		if(host == null) {
			return false;
		}
		local.setHost(host);
		// 解析并且设置可接受的远程关闭主机地址
		if(!loadShutdown(document)) {
			return false;
		}

		// 各节点在本地的监听地址
		Element elem = (Element)document.getElementsByTagName("listen-list").item(0);
		NodeList list = elem.getElementsByTagName("node");
		int size = list.getLength();
		for (int index = 0; index < size; index++) {
			Element e = (Element) list.item(index);
			String tag = xml.getValue(e, "tag");
			int port = Integer.parseInt(xml.getValue(e, "port"));
			
			LogNode node = null;
			if ("TOP".equalsIgnoreCase(tag)) {
				node = new LogNode(Site.TOP_SITE, port);
			} else if ("HOME".equalsIgnoreCase(tag)) {
				node = new LogNode(Site.HOME_SITE, port);
			} else if ("DATA".equalsIgnoreCase(tag)) {
				node = new LogNode(Site.DATA_SITE, port);
			} else if ("CALL".equalsIgnoreCase(tag)) {
				node = new LogNode(Site.CALL_SITE, port);
			} else if ("WORK".equalsIgnoreCase(tag)) {
				node = new LogNode(Site.WORK_SITE, port);
			} else if("BUILD".equalsIgnoreCase(tag)) {
				node = new LogNode(Site.BUILD_SITE, port);
			}
			if(node == null) {
				System.out.printf("Launcher.loadLocal, unknown log node: '%s'\n", tag);
				return false;
			}
			// 保存节点配置
			local.add(node);
		}
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length > 0) {
			String filename = args[0];
			boolean success = Launcher.getInstance().loadLocal(filename);
			if (success) {
				Launcher.getInstance().start();
			}
		}
	}
}