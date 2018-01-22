/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * ICMP interface, scan remote address
 * 
 * @author bill.liu lexst@126.com
 * @version 1.0 12/2/2009
 * 
 * @see com.lexst.util.host
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.util.host;

public class Echo {

	static {
		try {
			System.loadLibrary("lexstecho");
		} catch (UnsatisfiedLinkError exp) {
			exp.printStackTrace();
		} catch (SecurityException exp) {
			exp.printStackTrace();
		}
	}
	
	/**
	 * test hop number from dest address
	 * on success, hop number return; otherwise -1 return
	 * 
	 * @param remote
	 * @return
	 */
	public native static int step(byte[] remote);
	
	/**
	 * test echo time from dest address
	 * on success, ping time return (>=0); otherwise -1 return
	 * 
	 * @param remote
	 * @return
	 */
	public native static int reply(byte[] remote);

	public native static byte[] ping(byte[] remote);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		String value = System.getProperty("java.library.path");
//		System.out.printf("address is %s\n", value);

		String remote = "127.0.0.1";

//		int hops = Echo.step(remote.getBytes());
//		System.out.printf("echo hop %d\n", hops);
//
//		int time = Echo.reply(remote.getBytes());
//		System.out.printf("ping time %d\n", time);

//		int value = 0x100007f;
//		value = 0x16777343;
//		for(int off = 24; off >=0; off -=8) {
//			System.out.printf("%d ", (value >> off) & 0xff);
//		}
//		System.out.println();
		
		byte[] b = Echo.ping(remote.getBytes());
		System.out.printf("return value is:'%s'\n", (b == null ? "NULL" : new String(b)));
	}

}