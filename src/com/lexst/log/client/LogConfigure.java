/**
 *
 */
package com.lexst.log.client;

import java.io.*;

import org.w3c.dom.*;

import com.lexst.xml.*;

/**
 * 日志配置资料
 *
 */
public final class LogConfigure {
	
	/** 日志目标位置 **/
	private final static int NONE = 0;
	private final static int BUFFER = 1;
	private final static int FILE = 2;
	private final static int SERVER = 3;

	/** 是否在控制台打印 **/
	private boolean print;

	/** 日志发送模式 **/
	private int sendmode;

	/** 本地日志缓存区 **/
	private int buffsize;

	/** 日志在本地保存超时时间 **/
	private int timeout;

	/** 本地日志写入目录 **/
	private File logPath;

	/** 本地保存时，日志文件最大尺寸 */
	private int filesize;

	/** 日志级别，低于这个级别的日志不发送 **/
	private int level;

	/**
	 * 初始化配置
	 */
	public LogConfigure() {
		super();
		this.print = false;
		this.sendmode = BUFFER;
		this.level = LogLevel.debug;
	}

	/**
	 * 是否在本地终端打印日志
	 * @return
	 */
	public boolean isPrint() {
		return this.print;
	}

	/**
	 * 日志发送模式(none, file, server)
	 * @return
	 */
	public int getMode() {
		return this.sendmode;
	}

	public boolean isNoneMode() {
		return sendmode == LogConfigure.NONE;
	}
	
	public boolean isBufferMode() {
		return sendmode == LogConfigure.BUFFER;
	}

	public boolean isFileMode() {
		return sendmode == LogConfigure.FILE;
	}
	
	public boolean isServerMode() {
		return sendmode == LogConfigure.SERVER;
	}

	/**
	 * 本地日志写入目录
	 * @return
	 */
	public File getDirectory() {
		return this.logPath;
	}

	/**
	 * 本地日志最大保存尺寸
	 * @return
	 */
	public int getFileSize() {
		return this.filesize;
	}

	/**
	 * 本地日志缓存区尺寸
	 * @return
	 */
	public int getBufferSize() {
		return this.buffsize;
	}

	/**
	 * 缓存区日志超时时间(超时即发送)
	 * @return
	 */
	public int getTimeout() {
		return this.timeout;
	}

	/**
	 * 日志发送级别
	 * @return
	 */
	public int getLevel() {
		return this.level;
	}
	
	/**
	 * 设置日志发送级别
	 * @param value
	 */
	public void setLevel(int value) {
		if (!LogLevel.isLevel(level)) {
			throw new IllegalArgumentException("invalid log level:" + level);
		}
		this.level = value;
	}

	/**
	 * 加载并且解析日志配置文件
	 * @return boolean
	 */
	public boolean loadXML(String filename) {
		File file = new File(filename);
		if (!file.exists()) {
			return false;
		}
		byte[] b = new byte[(int) file.length()];
		try {
			FileInputStream in = new FileInputStream(file);
			in.read(b, 0, b.length);
			in.close();
			return loadXML(b);
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		return false;
	}

	/**
	 * 解析日志配置文件
	 * @param data
	 * @return
	 */
	public boolean loadXML(byte[] data) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(data);
		if (document == null) {
			return false;
		}

		NodeList list = document.getElementsByTagName("log");
		Element elem = (Element) list.item(0);

		String s = xml.getValue(elem, "level");
		if ("DEBUG".equalsIgnoreCase(s)) {
			level = LogLevel.debug;
		} else if ("INFO".equalsIgnoreCase(s)) {
			level = LogLevel.info;
		} else if ("WARNING".equalsIgnoreCase(s)) {
			level = LogLevel.warning;
		} else if ("ERROR".equalsIgnoreCase(s)) {
			level = LogLevel.error;
		} else if ("FATAL".equalsIgnoreCase(s)) {
			level = LogLevel.fatal;
		} else {
			throw new IllegalArgumentException("invalid log level!");
		}

		s = xml.getValue(elem, "console-print");
		print = "YES".equalsIgnoreCase(s);

		s = xml.getValue(elem, "directory");
		this.logPath = new File(s);

		s = xml.getValue(elem, "filesize");
		filesize = Integer.parseInt(s) * 1024 * 1024;

		s = xml.getValue(elem, "send-mode");

		if ("SERVER".equalsIgnoreCase(s)) {
			sendmode = LogConfigure.SERVER;
		} else if ("FILE".equalsIgnoreCase(s)) {
			sendmode = LogConfigure.FILE;
		} else if ("NONE".equalsIgnoreCase(s)) {
			sendmode = LogConfigure.NONE;
		} else {
			throw new IllegalArgumentException("invalid send mode! " + s);
		}
		s = xml.getValue(elem, "buffer-size");
		buffsize = Integer.parseInt(s) * 1024;
		s = xml.getValue(elem, "send-interval");
		timeout = Integer.parseInt(s);
		return true;
	}

}