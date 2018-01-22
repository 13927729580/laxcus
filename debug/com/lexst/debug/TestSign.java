/**
 *
 */
package com.lexst.debug;

import java.util.*;
/**
 * @author siven
 *
 */
public class TestSign implements java.lang.Runnable {

	private Thread thread;

	/**
	 *
	 */
	public TestSign() {
		// TODO Auto-generated constructor stub
	}

	public void start() {
		thread = new Thread(this);
		thread.start();
	}

	public void test() {
		TreeSet<Long> set = new TreeSet<Long>();

		byte[] b = new byte[128];
		java.util.Random rand = new java.util.Random(System.currentTimeMillis());
		int num = 13500;
		int count = 0;
		long begin = System.currentTimeMillis();
		for (int i = 0; i < num; i++) {
			rand.nextBytes(b);
			long value = com.lexst.util.Sign.sign(b, 0, b.length);
			if (set.contains(value)) {
				count++;
				continue;
			}
			set.add(value);
		}
		long usedtime = System.currentTimeMillis() - begin;

		System.out.printf("memory size:%d\n", set.size());
		set.clear();

		System.out.printf("count:%d, num:%d\n", count, num);
		System.out.printf("used time:%d\n", usedtime);
	}

	public void test2() {
		TreeSet<Long> set = new TreeSet<Long>();

		byte[] b = "linux-systes-pentium".getBytes();
		java.util.Random rand = new java.util.Random(System.currentTimeMillis());
		int num = 50;
		int count = 0;
		long begin = System.currentTimeMillis();
		for (int i = 0; i < num; i++) {
//			rand.nextBytes(b);
			long value = com.lexst.util.Sign.sign(b, 0, b.length);
			System.out.printf("value: %d - %X\n", value, value);
			if (set.contains(value)) {
				count++;
				continue;
			}
			set.add(value);
		}
		long usedtime = System.currentTimeMillis() - begin;
		set.clear();

		System.out.printf("count:%d, num:%d\n", count, num);
		System.out.printf("used time:%d\n", usedtime);
	}

	public void test3() {
		byte[] b = new byte[123];
		for (int i = 0; i < b.length; i++) {
			b[i] = (byte) 'd';
		}

		int num = 50;
		for (int i = 0; i < num; i++) {
			long value = com.lexst.util.Sign.sign(b, 0, b.length);
			System.out.printf("value: %d - %X\n", value, value);
		}
	}

	public void test4() {
		String path = "c:/windows";
		byte[] b = path.getBytes();

		for (int i = 0; i < 3; i++) {
			int ret1 = com.lexst.data.Install.setChunkRoot(b);
			int ret2 = com.lexst.data.Install.initialize();
			System.out.printf("set path, return is:%d - %d\n", ret1, ret2);
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		this.test();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		for (int i = 0; i < 20; i++) {
			TestSign ts = new TestSign();
			ts.test();
//			ts.test2();
//			ts.test3();
//			ts.start();
			ts.test4();
		}
	}

}