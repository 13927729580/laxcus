/**
 *
 */
package com.lexst.debug;

import com.lexst.home.*;

/**
 * @author siven
 *
 */
public class TestHome {

	/**
	 *
	 */
	public TestHome() {
		// TODO Auto-generated constructor stub
	}

	public synchronized void delay(long timeout) {
		try {
			super.wait(timeout);
		} catch (java.lang.InterruptedException exp) {

		}
	}

	public void test() {
//		String filename = "D:/workspace/lexst/src/com/lexst/home/local.xml";
//		Launcher.getInstance().loadXML(filename);
//		Launcher.getInstance().start();
//		this.delay(10000);
//		Launcher.getInstance().stop();
//		System.out.println("finished home site");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TestHome t = new TestHome();
		t.test();
	}

}
