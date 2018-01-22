/**
 *
 */
package com.lexst.call;

/**
 * 基于WEB环境(如TOMCAT)的启动器，作为一个应用包存在。<br>
 *
 */
public class WebLauncher extends CallLauncher {

	// static handle
	private static WebLauncher selfHandle = new WebLauncher();

	/**
	 * default
	 */
	private WebLauncher() {
		super();
		super.setExitVM(false);
		super.setLogging(true);
	}

	/**
	 * return the WebLauncher of handle
	 */
	public static WebLauncher getInstance() {
		return WebLauncher.selfHandle;
	}

}