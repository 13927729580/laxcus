/**
 *
 */
package com.lexst.debug;

import com.lexst.sql.schema.*;

/**
 * @author siven
 *
 */
public class TestAccount {

	/**
	 *
	 */
	public TestAccount() {
		// TODO Auto-generated constructor stub
	}

	public void test() {
		String db = "CPU";
		String table = "Pentium";
		String pwd = "Slide";
		Space space = new Space(db, table);
		Table schema = new Table(space);

		Schema base = new Schema(db);

		boolean b = true; //com.lexst.home.Launcher.getInstance().createDB(base);
		System.out.printf("create db %b\n", b);
		com.lexst.home.Launcher.getInstance().createSpace(schema);
		System.out.printf("create table %b\n", b);

		b = com.lexst.home.Launcher.getInstance().deleteSpace(new Space(db, table));
		System.out.printf("delete table %b\n", b);

		b = true; //com.lexst.home.Launcher.getInstance().deleteDB(db);
		System.out.printf("delete db %b\n", b);
	}

	public static void main(String[] args) {
		TestAccount test = new TestAccount();
		for(int i =0; i<10; i++)
		test.test();
	}
}
