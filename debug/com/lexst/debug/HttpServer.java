/**
 * 
 */
package com.lexst.debug;

import java.io.*;
import java.net.*;

/**
 * @author siven
 *
 */
public class HttpServer {
	
	private java.net.ServerSocket socket;
	private int port = 7080;

	/**
	 * 
	 */
	public HttpServer() {
		// TODO Auto-generated constructor stub
	}
	
	private synchronized void delay(long timeout) {
		try {
			this.wait(timeout);
		} catch (java.lang.InterruptedException exp) {
			exp.printStackTrace();
		}
	}
	
	public void receive() throws IOException {
		socket = new ServerSocket(port);
		System.out.println("bind to "+port);
		Socket client = socket.accept();
		System.out.println("accpeted a socket");
		
		InputStream in = client.getInputStream();
		
		this.delay(2000);
		
//		byte[] data = new byte[1024];
//		ByteArrayOutputStream out = new ByteArrayOutputStream();
//		for (int i = 0; i < 2; i++) {
//			int size = in.read(data, 0, data.length);
//			if (size == -1) break;
//			System.out.printf("%d receive data size %d\n", i + 1, size);
//			out.write(data, 0, size);
//		}
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] data = new byte[1024 * 500];
		int size = in.read(data, 0, data.length);
//		out.write(data, 0, size);
		
//		this.delay(2000);
//		size = in.read(data, 0, data.length);
//		out.write(data, 0, size);
		
		System.out.printf("receive size %d\n", size);
		System.out.println("exit receive!");
		
		String s = new String(data, 0, size);
		System.out.println(s);

		client.close();
		socket.close();
		
//		byte[] b = out.toByteArray();
//		String s = new String(b, 0, b.length, "GBK");
//		System.out.println(s);
		
//		System.out.println(new String(b));
	}
	
	public static void main(String[] args) {
		HttpServer server = new HttpServer();
		try {
			server.receive();
		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}

}
