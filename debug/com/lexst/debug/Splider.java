/**
 *
 */
package com.lexst.debug;


/**
 * @author siven
 *
 */
public class Splider {

	public Splider() {
		// TODO Auto-generated constructor stub
	}

	public void test() {
		java.io.Console console = System.console();
		if(console == null) {
			System.out.println("console is invalid!");
			return;
		}

		boolean running = false;
		while( running ) {
			String s = console.readLine();
			if(s.equals("exit")) {
				running = true;
			} else {
				System.out.printf("this is:[%s]\n", s);
			}
		}
	}

	public static void main(String[] args) {
		new Splider().test();
	}
}
