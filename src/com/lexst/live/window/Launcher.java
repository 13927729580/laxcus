/**
 * 
 */
package com.lexst.live.window;

import com.lexst.algorithm.collect.*;
import com.lexst.live.*;
import com.lexst.live.pool.*;
import com.lexst.log.client.*;
import com.lexst.site.live.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;

public class Launcher extends BasicLauncher implements LiveListener {

	private static Launcher selfHandle = new Launcher();

	// local server address
	private LiveSite local = new LiveSite();
	// sqlive window
	private SQLWindow window = new SQLWindow();
	
	private boolean flicker = false;
	
	/**
	 * default constructor
	 */
	private Launcher() {
		super();
		super.setExitVM(true);
		super.setLogging(true);
		this.initIP();
		// cannot print log
		fixpStream.setPrint(false);
		fixpPacket.setPrint(false);
		// init invoker
		streamImpl = new LiveStreamInvoker();
		packetImpl = new LivePacketInvoker(this, fixpPacket);
	}

	/**
	 * @return
	 */
	public static Launcher getInstance() {
		return Launcher.selfHandle;
	}
	
	public SQLWindow getFrame() {
		return this.window;
	}
	
	/**
	 * get local site 
	 * @return
	 */
	public LiveSite getLocal() {
		return this.local;
	}
	
	private void initIP() {
		local.setHost(Address.select(), 0, 0);
	}
	
	/**
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

		while (CollectTaskPool.getInstance().isRunning()) {
			this.delay(300);
		}
		while (TouchPool.getInstance().isRunning()) {
			this.delay(300);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		Logger.debug("Launcher.init, local host %s", local.getHost());
		
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
		Logger.debug("Launcher.init, new local host %s", local.getHost());
		//3. show window, and login
		if (success) {
			success = window.showWindow();
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
		Logger.info("Launcher.process, into ...");
		while(!super.isInterrupted()) {
			this.delay(1000);
			if(flicker) {
				flicker = false;
				window.flicker();
			}
		}
		Logger.info("Launcher.process, exit ...");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		this.stopPool();
		CollectTaskPool.getInstance().stop();
		this.stopListen();
		// close
		window.dispose();
	}

	/* (non-Javadoc)
	 * @see com.lexst.live.LiveListener#flicker()
	 */
	@Override
	public void flicker() {
		flicker = true;
		this.wakeup();
		// notify top pool, active reply
		TouchPool.getInstance().replyActive();
	}

	/* (non-Javadoc)
	 * @see com.lexst.live.LiveListener#shutdown()
	 */
	@Override
	public void shutdown() {
		this.stop();
	}

	/* top timeout, stop service
	 * @see com.lexst.live.LiveListener#disconnect()
	 */
	@Override
	public void disconnect() {
		window.disconnect();
	}

	/* 
	 * notify top site, send "hello" command
	 * @see com.lexst.live.LiveListener#active(int, com.lexst.util.host.SocketHost)
	 */
	@Override
	public void active(int num, SocketHost remote) {
		super.hello(num, local.getFamily(), remote);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Launcher.getInstance().start();
	}

}