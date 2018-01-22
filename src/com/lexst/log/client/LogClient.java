/**
 *
 */
package com.lexst.log.client;

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;

import com.lexst.fixp.*;
import com.lexst.fixp.client.FixpPacketClient;
import com.lexst.thread.*;
import com.lexst.util.host.*;

/**
 * 日志发送客户端，负责日志的接收，判断是否发送，本地保存，传送(写入本地或者发送到日志服务器)
 *
 */
public final class LogClient extends VirtualThread {
	/** 日志文件后缀 **/
	private final static String suffix = ".log";

	/** 日志服务器地址 **/
	private SocketHost remote = new SocketHost(SocketHost.UDP);

	/** 日志发送客户端 **/
	private FixpPacketClient client = new FixpPacketClient();

	/** 日志文件 **/
	private File diskfile;

	/** 当前日期，默认是-1未定义 **/
	private int today;

	/** 日志保存缓冲区 **/
	private LogBuffer buff = new LogBuffer();

	/** 日志的时间格式 **/
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH);

	/** 日志配置 **/
	private LogConfigure config;

	/** 日志打印接口 **/
	private LogPrinter printer;

	/**
	 * default constructor
	 */
	public LogClient() {
		super();
		this.today = -1;
		config = new LogConfigure();
		client.setSubPacketTimeout(1000);	//keep timeout, 1000 millisecond
		client.setReceiveTimeout(120);	//120 second
	}

	public void setLogConfigure(LogConfigure cfg) {
		this.config = cfg;
		buff.ensure(config.getBufferSize());
	}

	public LogConfigure getLogConfigure() {
		return this.config;
	}
	
	public void setLogPrinter(LogPrinter s) {
		this.printer = s;
	}
	public LogPrinter getLogPrinter() {
		return this.printer;
	}

	/**
	 * 关闭SOCKET连接
	 */
	private void closeSocket() {
		client.close();
	}

	/**
	 * 建立本地日志目录
	 * @param path
	 */
	private boolean createDirectory(File path) {
		Logger.info("LogClient.createDirectory, directory is '%s'", path);
		if (!path.exists() || !path.isDirectory()) {
			return path.mkdirs();
		}
		return true;
	}

	/**
	 * 定义日志文件名
	 * @return boolean
	 */
	private boolean choose() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String today = df.format(new java.util.Date());
		for (int index = 1; index < Integer.MAX_VALUE; index++) {
			String name = String.format("%s(%d)%s", today, index, LogClient.suffix);
			File file = new File(config.getDirectory(), name);
			if (!file.exists()) {
				this.diskfile = file;
				return true;
			}
		}
		return false;
	}

	/**
	 * load udp connect
	 * @param ip
	 * @param port
	 * @throws SocketException
	 */
	private boolean bind() {
		try {
			return client.bind();
		} catch (IOException exp) {
			
		}
		return false;
	}

	/**
	 * @param config
	 * @param host (log server address)
	 * @return
	 * @throws IOException
	 */
	public boolean load(LogConfigure config, SiteHost host) {		
		boolean success = false;
		if (config.isNoneMode() || config.isBufferMode()) {
			setLogConfigure(config);
			success = true;
		} else if (config.isFileMode()) {
			setLogConfigure(config);
			createDirectory(config.getDirectory());
			success = this.choose();
		} else if(config.isServerMode()) {
			if (host == null) return false;
			remote.set(host.getPacketHost());
			success = bind();
			setLogConfigure(config);
		} else {
			throw new IllegalArgumentException("invalid log mode id!");
		}
		// load thread
		if (success) {
			// pre-print
			String s = buff.flush();
			if (config.isPrint() && !s.isEmpty()) {
				if (printer != null) {
					printer.print(s);
				} else {
					System.out.print(s);
				}
			}
			// start thread
			return start();
		}
		return success;
	}

	/**
	 * stop service
	 */
	public void stopService() {
		this.stop();
	}

	/**
	 * datagram send
	 * @param log
	 */
	private void send(String log) {
		byte[] b = toUTF8(log);
		if (b == null) return;

		Command cmd = new Command(Request.APP, Request.ADD_LOG);
		Packet packet = new Packet(remote, cmd);
		packet.setData(b, 0, b.length);
		try {
			Packet reply = client.batch(packet);
			cmd = reply.getCommand();
			if (cmd.getResponse() == Response.OKAY) {
				// success
			} else {
				// failed
			}
		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}
	
	private byte[] toUTF8(String log) {
		try {
			return log.getBytes("UTF-8");
		} catch (UnsupportedEncodingException exp) {

		}
		return null;
	}

	/**
	 * write text to local disk
	 * @param log
	 */
	private void writeLog(String log) {
		if (this.diskfile == null) {
			boolean success = this.choose();
			if (!success) return;
		}
		// 日志转换成UTF8编码
		byte[] b = toUTF8(log);
		if (b == null || b.length == 0) return;
		// 日志以追加方式写入磁盘
		try {
			FileOutputStream out = new FileOutputStream(diskfile, true);
			out.write(b, 0, b.length);
			out.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		} catch(Throwable exp) {
			exp.printStackTrace();
		}

		// 如果日志文件尺寸溢出，建立新文件
		if (diskfile.length() >= config.getFileSize()) {
			boolean success = this.choose();
			if (!success) {
				this.diskfile = null;
				return;
			}
		} else {
			// 如果日期变化也更新文件
			Calendar dar = Calendar.getInstance();
			dar.setTime(new java.util.Date(System.currentTimeMillis()));
			int day = dar.get(Calendar.DAY_OF_MONTH);
			if (this.today == -1) {
				this.today = day;
			} else if (this.today != day) {
				this.today = day;
				this.choose();
			}
		}
	}

	/**
	 * send data to disk or log server
	 */
	private void flush() {
		if(buff.isEmpty()) return;
		String s = buff.remove();
		if (config.isFileMode()) {
			this.writeLog(s); // write to disk
		} else if(config.isServerMode()) {
			this.send(s);	// send to log server
		}
	}

	/**
	 * 向日期服务器发送日志
	 * @param level
	 * @param log
	 */
	public void sendLog(int level, String log) {
		String s = String.format("%s: %s %s\r\n", LogLevel.getText(level), sdf.format(new Date()), log);
		// 是否控制打印，如果是可选择指定打印接口或者终端打印两种
		if (config.isPrint()) {
			if (printer != null) {
				printer.print(s);
			} else {
				System.out.print(s);
			}
		}
		// 如果日志不传递
		if (config.isNoneMode()) return;

		// 保存日志
		buff.append(s);
		// when temporary mode, save it
		if (config.isBufferMode()) {
			if (buff.length() >= 524288) buff.clear();
		} else if (buff.isFull()) {
			// 唤醒线程，输出到磁盘或者服务器
			this.wakeup();
		}
	}

	/**
	 * 输出日志到终端
	 */
	public void gushing() {
		String s = buff.remove();
		if (config.isBufferMode()) {
			if (s != null && s.length() > 0) {
				System.out.println(s);
			}
		}
	}

	/**
	 *
	 */
	@Override
	public boolean init() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("LogClient.process, timeout:%d, into...", config.getTimeout());
		this.setSleep(config.getTimeout());
		while (!isInterrupted()) {
			this.flush();
			this.sleep();
		}
		Logger.info("LogClient.process, exit");
		this.flush();
	}
	
	/**
	 *
	 */
	@Override
	public void finish() {
		// 关闭SOCKET连接
		this.closeSocket();
	}
}