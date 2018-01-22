/**
 * 
 */
package com.lexst.live.console;

import java.io.*;

import com.lexst.algorithm.collect.*;
import com.lexst.live.*;
import com.lexst.live.pool.*;
import com.lexst.log.client.*;
import com.lexst.site.live.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;

public class Launcher extends BasicLauncher implements LiveListener {

	private static Launcher selfHandle = new Launcher();

	/** 本地地址配置 **/
	private LiveSite local = new LiveSite();
	
	private Terminal terminal = new Terminal();
	
	/**
	 * default constructor
	 */
	private Launcher() {
		super();
		super.setExitVM(true);
		super.setLogging(true);
		this.initAddress();
		// cannot print log
		fixpStream.setPrint(false);
		fixpPacket.setPrint(false);
		// init invoker
		streamImpl = new LiveStreamInvoker();
		packetImpl = new LivePacketInvoker(this, fixpPacket);
		
		Logger.setLevel(LogLevel.none);
	}

	/**
	 * @return
	 */
	public static Launcher getInstance() {
		return Launcher.selfHandle;
	}
	
	/**
	 * @return
	 */
	public Console getConsole() {
		return terminal.getConsole();
	}

	/**
	 * get local site 
	 * @return
	 */
	public LiveSite getLocal() {
		return this.local;
	}
	
	/**
	 * 初始化本地绑定地址
	 */
	private void initAddress() {
		local.setHost(Address.select(), 0, 0);
	}
	
	/**
	 * load top pool
	 * @return
	 */
	private boolean loadPool() {
		boolean success = TouchPool.getInstance().start();
		if(success) {
			success = CollectTaskPool.getInstance().start();
		}
		return success;
	}

	/**
	 * stop top pool
	 */
	private void stopPool() {
		CollectTaskPool.getInstance().stop();
		TouchPool.getInstance().stop();
		
		while(CollectTaskPool.getInstance().isRunning()) {
			this.delay(300);
		}
		while (TouchPool.getInstance().isRunning()) {
			this.delay(200);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		if(!terminal.initialize()) {
			System.out.println("cannot init system console");
			return false;
		}
		
		TouchPool.getInstance().setLocal(local);
		TouchPool.getInstance().setLiveListener(this);
		CollectTaskPool.getInstance().setSleep(30); // 30 second check

		//1. load pool
		boolean success = loadPool();
		//2. load listen
		if (success) {
			success = loadListen(null, local.getHost());
			if (success) {
				SocketHost host = fixpStream.getLocal();
				local.getHost().setTCPort(host.getPort());
				host = fixpPacket.getLocal();
				local.getHost().setUDPort(host.getPort());
			} else {
				stopPool();
				return false;
			}
		}
		//3. show console and login
		if (success) {
			success = terminal.login();
			if(!success) {
				stopListen();
				stopPool();
			}
		}

		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		while(!isInterrupted()) {
			boolean exit = terminal.execute();
			if (exit) {
				setInterrupted(true);
				break;
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		this.stopPool();
		CollectTaskPool.getInstance().stop();
		this.stopListen();
	}

	/* (non-Javadoc)
	 * @see com.lexst.live.LiveListener#flicker()
	 */
	@Override
	public void flicker() {
		// TODO Auto-generated method stub
		TouchPool.getInstance().replyActive();
	}

	/* (non-Javadoc)
	 * @see com.lexst.live.LiveListener#shutdown()
	 */
	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		this.stop();
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.live.LiveListener#disconnect()
	 */
	@Override
	public void disconnect() {
		System.out.println("connect interrupted!");
	}

	/* (non-Javadoc)
	 * @see com.lexst.live.LiveListener#active(int, com.lexst.util.host.SocketHost)
	 */
	@Override
	public void active(int num, SocketHost topsite) {
		this.hello(num, local.getFamily(), topsite);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Launcher.getInstance().start();
	}

}