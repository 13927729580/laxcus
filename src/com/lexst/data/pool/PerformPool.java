/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2012 lexst.com. All rights reserved
 * 
 * platform checker, include: disk, cpu, memory, network, only linux system
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 8/18/2012
 * 
 * @see com.lexst.data.pool
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.data.pool;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import com.lexst.data.*;
import com.lexst.fixp.*;
import com.lexst.fixp.client.*;
import com.lexst.log.client.*;
import com.lexst.remote.client.home.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;

/**
 * 
 * 调用LINUX命令，显示当前的负载等情况。只限LINUX平台使用
 *
 */
public class PerformPool extends LocalPool {

	private static PerformPool selfHandle = new PerformPool();

	private boolean linux;
	
	/**
	 * 
	 */
	private PerformPool() {
		super();
		linux = false;
	}

	/**
	 * @return
	 */
	public static PerformPool getInstance() {
		return PerformPool.selfHandle;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		alter();
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("PerformPool.process, into...");
		while (!super.isInterrupted()) {
			check();
			this.delay(1000);
		}
		Logger.info("PerformPool.process, exit");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// TODO Auto-generated method stub
	}

	/**
	 * alter os name
	 */
	private void alter() {
		String os = System.getProperty("os.name");
		if (os != null) {
			linux = os.toLowerCase().indexOf("linux") > -1;
		}
	}
	
	private void check() {
		if(!linux) return;
		
		this.diskio();
	}
	
	private ArrayList<Integer> util = new ArrayList<Integer>();
	/**
	 * check disk jobs
	 */
	private void diskio() {
		String cmds = "iostat -d -x";
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		try {
			Process process = Runtime.getRuntime().exec(cmds);
			InputStream in = process.getInputStream();
			byte[] b = new byte[1024];
			while(true) {
				int size = in.read(b, 0, b.length);
				if(size < 1) break;
				buff.write(b, 0, size);
			}
			in.close();
		} catch (IOException exp) {
			Logger.error(exp);
		}
		byte[] b = buff.toByteArray();
		if (b == null || b.length < 1) return;
		String text = new String(b);
		
		BufferedReader reader = new BufferedReader(new StringReader(text));
		boolean into = false;
		while(true) {
			String line = null;
			try {
				line = reader.readLine();
			} catch(IOException exp) {
				Logger.error(exp);
			}
			if(line == null) break;
			if(into) {
				String regex = "^\\s*(?i)([a-z0-9]{2,})\\s+(.+)\\s+([0-9]{1,3}).([0-9]{1,3})\\s*$";
				Pattern pattern = Pattern.compile(regex);
				Matcher matcher = pattern.matcher(line);
				if(matcher.matches()) {
					String s3 = matcher.group(3);
					util.add(Integer.parseInt(s3));
				}
			} else {
				String regex = "^\\s*(?i)(Device:)(.+)(?i)(%util)\\s*$";
				Pattern pattern = Pattern.compile(regex);
				Matcher matcher = pattern.matcher(line);
				into = matcher.matches();
			}
		}
		if(util.size()<11) return;
		
		util.remove(0);
		int count = 0;
		for(int value : util) {
			count+= value;
		}
		int seg = count / util.size();
		int limit = 80;
		if(seg < limit) return; //小于80, 低于指标值,不处理
		
		HashSet<SiteHost> all = new HashSet<SiteHost>();
		List<Space> list = Launcher.getInstance().listSpace();
		HomeClient client = this.bring(Launcher.getInstance().getHubSite());

		try {
			for (Space space : list) {
				SiteHost[] hosts = client.findCallSite(space);
				for (int i = 0; hosts != null && i < hosts.length; i++) {
					all.add(hosts[i]);
				}
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}

		super.complete(client);

		// send packet to call site
		SiteHost local = Launcher.getInstance().getLocal().getHost();
		FixpPacketClient fixp = new FixpPacketClient();
		
		for (SiteHost host : all) {
			Command cmd = new Command(Request.NOTIFY, Request.OVERLOAD);
			Packet packet = new Packet(host.getPacketHost(), cmd);
			packet.addMessage(Key.LOCAL_ADDRESS, local.toString());
			try {
				for (int i = 0; i < 3; i++) {
					fixp.send(packet);
				}
			} catch (IOException exp) {
				Logger.error(exp);
			}
		}
		fixp.close();
		
		// 准备重新开始
		util.clear();
	}

}