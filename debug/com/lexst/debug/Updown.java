/**
 * 
 */
package com.lexst.debug;

import java.io.*;
import java.net.*;

import com.lexst.remote.client.data.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;

/**
 * @author siven
 *
 */
public class Updown implements Runnable {
	
	private ServerSocket server;
	
	private Thread thread;
	
	private int port = 8888;

	/**
	 * 
	 */
	public Updown() {
		super();
	}
	
	private synchronized void delay(long timeout) {
		try {
			this.wait(timeout);
		} catch (java.lang.InterruptedException exp) {
			exp.printStackTrace();
		}
	}
	
	private void close() throws IOException {
		server.close();
	}
	
	
	private void bind() throws IOException {
		server = new ServerSocket(port);
	}
	
	private Socket accept() throws IOException {
		return server.accept();
	}
	
	public void start() {
		thread = new Thread(this);
		thread.start();
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		System.out.println("into thread...");
		try {
			this.bind();
			Socket socket = accept();
			this.upload(socket);
			this.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		System.out.println("exit thread...");
		thread = null;
	}

	private void upload(Socket socket) throws IOException {
		InputStream in = socket.getInputStream();
		OutputStream out = socket.getOutputStream();
		
		long chunkId = Long.MAX_VALUE;
		Space space = new Space("Video", "Word");
		String filename = "f:/tmp.xml";
		
		// read content
		byte[] b = new byte[1024];
		int size = in.read(b, 0, b.length);
		this.print(b, 0, size);
		
//		Uploader up = new Uploader();
//		up.execute(space, chunkId, filename, out);
		// close socket
		socket.close();
	}
	
	private void download() throws IOException {
		DataDownloader down = new DataDownloader();
		SocketHost remote = new SocketHost("127.0.0.1", this.port);
		Space space = new Space("Video", "Word");
		long chunkId = Long.MAX_VALUE;
		String filename = "c:/abc.xml";
		
		down.execute(remote, space, chunkId, filename);
	}
	
	public void test() {
		this.start();
		this.delay(1000);
		try {
			this.download();
		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}
	
	private void print(byte[] b, int off, int len) {
		StringBuilder buff = new StringBuilder();
		int end = off + len;
		for (int i = off; i < end; i++) {
			String s = String.format("%x", b[i] & 0xff);
			if (s.length() == 1) s = "0" + s;
			if (buff.length() > 0) buff.append(" ");
			buff.append(s);
		}
		System.out.println(buff.toString());
	}
	
	public static void main(String[] args) {
		Updown ud = new Updown();
		ud.test();
	}
}