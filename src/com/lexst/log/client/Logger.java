/**
 * log client interface
 */
package com.lexst.log.client;

import java.io.*;

import com.lexst.util.host.*;

/**
 * 客户端日志发送接口<br>
 * @author scott.liang - laxcus.com
 *
 * @version 1.1
 */
public final class Logger {

	/** 日志资源配置 */
	private static LogConfigure logCfg = new LogConfigure();

	/** 日志客户端，发送数据到日志服务器 **/
	private static LogClient client = new LogClient();

	/**
	 * construct method
	 */
	private Logger() {
		super();
	}

	/**
	 * 设置日志打印接口(打印接口由用户定义，可以是终端、图形界面、或其它界面)
	 * @param s
	 */
	public static void setLogPrinter(LogPrinter s) {
		Logger.client.setLogPrinter(s);
	}

	/**
	 * 返回日志打印接口
	 * @return
	 */
	public static LogPrinter getLogPrinter() {
		return Logger.client.getLogPrinter();
	}

	/**
	 * 检测是否处于启动运行状态
	 * @return
	 */
	public static boolean isRunning() {
		return Logger.client.isRunning();
	}

	/**
	 * 加载本地日志配置资源
	 * @param filename
	 * @return
	 */
	public static boolean loadXML(String filename) {
		return Logger.logCfg.loadXML(filename);
	}
	
	/**
	 * @param data
	 * @return
	 */
	public static boolean loadXML(byte[] data) {
		return Logger.logCfg.loadXML(data);
	}

	/**
	 * 设置日志级别，依次是:debug, infor, warning, error, fatal。<br>
	 * 低于指定级别的日志不发送。<br>
	 * 
	 * @param level
	 */
	public static void setLevel(int level) {
		Logger.logCfg.setLevel(level);
	}

	/**
	 * 启动日志服务
	 * @param remote
	 * @return
	 */
	public static boolean loadService(SiteHost remote) {
		if(Logger.client.isRunning()) {
			Logger.warning("log client is running!");
			return false;
		}
		Logger.info("set log level '%s'", LogLevel.getText(Logger.logCfg.getLevel()));
		return Logger.client.load(Logger.logCfg, remote);
	}

	/**
	 * 停止日志服务
	 */
	public static void stopService() {
		Logger.client.stopService();
		while (Logger.client.isRunning()) {
			Logger.client.delay(500);
		}
	}

	/**
	 * 发送"debug"状态日志 
	 * @param log
	 */
	public static void debug(String log) {
		if (Logger.logCfg.getLevel() <= LogLevel.debug) {
			Logger.client.sendLog(LogLevel.debug, log);
		}
	}

	/**
	 * 格式化"debug"状态日志并且发送 
	 * @param format
	 * @param args
	 */
	public static void debug(String format, Object ... args) {
		String s = String.format(format, args);
		Logger.debug(s);
	}

	/**
	 * 发送"information"状态日志 
	 * @param log
	 */
	public static void info(String log) {
		if (Logger.logCfg.getLevel() <= LogLevel.info) {
			Logger.client.sendLog(LogLevel.info, log);
		}
	}

	/**
	 * write information log
	 * @param format
	 * @param args
	 */
	public static void info(String format, Object ... args) {
		String s = String.format(format, args);
		Logger.info(s);
	}

	/**
	 * write warning log
	 * @param log
	 */
	public static void warning(String log) {
		if (Logger.logCfg.getLevel() <= LogLevel.warning) {
			Logger.client.sendLog(LogLevel.warning, log);
		}
	}

	/**
	 * write warning log
	 * @param format
	 * @param args
	 */
	public static void warning(String format, Object ... args) {
		String s = String.format(format, args);
		Logger.warning(s);
	}

	/**
	 * write error log
	 * @param log
	 */
	public static void error(String log) {
		if (Logger.logCfg.getLevel() <= LogLevel.error) {
			Logger.client.sendLog(LogLevel.error, log);
		}
	}
	
	/**
	 * flush throw text to string
	 * @param e
	 * @return
	 */
	private static String throwText(Throwable e) {
		if (e == null) return "";
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		PrintStream s = new PrintStream(out, true);
		e.printStackTrace(s);
		byte[] data = out.toByteArray();
		return new String(data, 0, data.length);
	}

	/**
	 * write error log
	 * @param log
	 * @param exp
	 */
	public static void error(String log, Throwable e) {
		StringBuilder buff = new StringBuilder(1024 * 3);
		buff.append(String.format("%s - ", log));
		buff.append(Logger.throwText(e));
		Logger.error(buff.toString());
	}

	/**
	 * error log
	 * @param t
	 */
	public static void error(Throwable t) {
		Logger.error(Logger.throwText(t));
	}

	/**
	 * write error log
	 * @param format
	 * @param args
	 */
	public static void error(String format, Object ... args) {
		String s = String.format(format, args);
		Logger.error(s);
	}

	/**
	 * write error log
	 * @param handle
	 * @param format
	 * @param args
	 */
	public static void error(Throwable handle, String format, Object ... args) {
		String s = String.format(format, args);
		Logger.error(s, handle);
	}

	/**
	 * write fatal log
	 * @param log
	 */
	public static void fatal(String log) {
		if (Logger.logCfg.getLevel() <= LogLevel.fatal) {
			Logger.client.sendLog(LogLevel.fatal, log);
		}
	}

	/**
	 * write fatal log
	 * @param log
	 * @param exp
	 */
	public static void fatal(String log, Throwable t) {
		StringBuilder buff = new StringBuilder(1024 * 3);
		buff.append(String.format("%s - ", log));
		buff.append(Logger.throwText(t));
		Logger.fatal(buff.toString());
	}

	/**
	 * write fatal log
	 * @param t
	 */
	public static void fatal(Throwable t) {
		Logger.fatal(Logger.throwText(t));
	}

	/**
	 * write fatal log
	 * @param format
	 * @param args
	 */
	public static void fatal(String format, Object ... args) {
		String s = String.format(format, args);
		Logger.fatal(s);
	}

	/**
	 * fatal log
	 * @param handle
	 * @param format
	 * @param args
	 */
	public static void fatal(Throwable handle, String format, Object ... args) {
		String s = String.format(format, args);
		Logger.fatal(s, handle);
	}

	/**
	 * write note
	 * @param prefix
	 * @param success
	 */
	public static void note(String prefix, boolean success) {
		if (success) {
			Logger.info(prefix + " success");
		} else {
			Logger.error(prefix + " failed");
		}
	}
	
	/**
	 * write note
	 * 
	 * @param success
	 * @param format
	 * @param args
	 */
	public static void note(boolean success, String format, Object... args) {
		String s = String.format(format, args);
		if (success) {
			Logger.info(s + " success");
		} else {
			Logger.error(s + " failed");
		}
	}

	/**
	 * flush log text to console
	 */
	public static void gushing() {
		Logger.client.gushing();
	}
}