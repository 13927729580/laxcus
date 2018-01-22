/**
 * 
 */
package com.lexst.debug;

import java.io.*;

/**
 * @author siven
 *
 */
public class Ping {

	private java.net.DatagramSocket client = null;
	
	/**
	 * 
	 */
	public Ping() {
		// TODO Auto-generated constructor stub
	}
	
	public void send(int num) throws java.io.IOException {
		byte[] b = "操!你丫别PING我!炸死你丫挺的!".getBytes();

		String ip = "172.16.249.188";
		int port = 8000;
		java.net.InetSocketAddress address = new java.net.InetSocketAddress(ip,
				port);
		java.net.DatagramPacket packet = new java.net.DatagramPacket(b,
				b.length, address);

		client = new java.net.DatagramSocket();
		client.connect(address);
		for (int i = 0; i < num; i++) {
			client.send(packet);
		}

		client.close();
		
		System.out.println("send finished!");
	}

	public static void main(String[] args) {
		
		Ping ping = new Ping();
		for(int i = 0; i < 10000; i++) {
		try {
			ping.send(100000);
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		}
		System.out.println("KAO!");
	}
}
