/**
 * 
 */
package com.lexst.log.server;

import java.io.*;
import java.text.*;
import java.util.*;

import com.lexst.fixp.*;
import com.lexst.invoke.*;
import com.lexst.util.host.SocketHost;

/**
 * 磁盘日志写入器
 *
 */
final class LogPacketWriter implements PacketInvoker {

	/** 磁盘日志文件的最大尺寸 **/
	private final static long maxFilesize = 10 * 1024 * 1024;
	
	/** 日志基础目录 */
	private File root;
	
	/** 当前天数 */
	private int today;
	
	/**
	 * default
	 */
	public LogPacketWriter() {
		super();
		this.today = -1;
	}
	
	/**
	 * 
	 * @param root
	 */
	public LogPacketWriter(File root) {
		this();
		this.setRoot(root);
	}
	
	/**
	 * 设置基础目录
	 * @param s
	 */
	public void setRoot(File s) {
		this.root = s;
	}
	
	/**
	 * 返回基础目录
	 * @return
	 */
	public File getRoot() {
		return this.root;
	}
	
	
	/* (non-Javadoc)
	 * @see com.lexst.invoke.PacketInvoker#invoke(com.lexst.fixp.Packet)
	 */
	@Override
	public Packet invoke(Packet packet) {
		SocketHost remote = packet.getRemote();
		Command cmd = packet.getCommand();
		
		if (cmd.getMajor() == Request.APP && cmd.getMinor() == Request.ADD_LOG) {
			byte[] logs = packet.getData();
			// 写日志到磁盘
			boolean success = writeLog(remote, logs);
			// 返回应答包
			cmd = new Command(success ? Response.OKAY : Response.SERVER_ERROR);
		} else {
			cmd = new Command(Response.UNSUPPORT);
		}
		Packet resp = new Packet(remote, cmd);
		return resp;
	}


	/**
	 * 根据IP地址建立对应的目录
	 * @param remote (node address)
	 * @return
	 */
	private File mkdirs(SocketHost remote) {
		String name = String.format("%s_%d", remote.getSpecifyAddress(), remote.getPort());
		File dir = new File(this.root, name);
		// 目录存在即返回
		if (dir.exists() && dir.isDirectory()) {
			return dir;
		}
		// 目录不存在，建立新目录
		boolean success = dir.mkdirs();
		return success ? dir : null;
	}

	/**
	 * 生成日志文件
	 * @param remote
	 * @return
	 */
	private File mkfile(SocketHost remote) {
		// 建立目录
		File logpath = mkdirs(remote);
		if (logpath == null) {
			return null;
		}
		
		// 根据当前日期建立日志文件
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String today = df.format(new Date(System.currentTimeMillis()));
		File file = null;
		int index = 1;
		for (; true; index++) {
			String suffix = String.format("%s(%d).log", today, index);
			File temp = new File(logpath, suffix);
			if (!temp.exists()) {
				if (file == null) file = temp;
				break;
			} else if (temp.length() < LogPacketWriter.maxFilesize) {
				file = temp; // next file
			}
		}
		return file;
	}

	/**
	 * 写日志到磁盘文件
	 * 
	 * @param logs
	 * @param len
	 */
	private boolean writeLog(SocketHost remote, byte[] logs) {
		boolean success = false;
		File filename = this.mkfile(remote);
		if(filename == null) return false;
		try {
			FileOutputStream writer = new FileOutputStream(filename, true);
			writer.write(logs, 0, logs.length);
			writer.flush();
			writer.close();
			success = true;
		} catch (Throwable exp) {
			exp.printStackTrace();
		}

		// 检查日志，如果不一致即更新
		Calendar dar = Calendar.getInstance();
		dar.setTime(new Date(System.currentTimeMillis()));
		int day = dar.get(Calendar.DAY_OF_MONTH);
		if (today == -1 || today != day) {
			today = day;
		}
		return success;
	}

}