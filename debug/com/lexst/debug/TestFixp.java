/**
 *
 */
package com.lexst.debug;

import java.io.IOException;
import java.net.*;

import com.lexst.fixp.*;
import com.lexst.fixp.client.*;
import com.lexst.fixp.monitor.*;
import com.lexst.util.host.*;

/**
 * @author siven
 *
 */
public class TestFixp {

	/**
	 *
	 */
	public TestFixp() {
		// TODO Auto-generated constructor stub
	}

	public synchronized void delay(long timeout) {
		try {
			super.wait(timeout);
		} catch (java.lang.InterruptedException exp) {

		}
	}

	public void doing() throws IOException {
		String ip = "localhost";
		int port = 8000;
		FixpPacketMonitor monitor = new FixpPacketMonitor(InetAddress.getByName(ip) , port);

		boolean success = monitor.start();
		System.out.printf("fixp thread start is:%b\n", success);

		this.delay(2000);
		this.sendPacket();
		this.delay(20000);

		monitor.stop();

		System.out.println("fixp monitor finished!");
	}

	public void sendPacket() {
//		SocketHost host = new SocketHost(SocketHost.UDP, ip, 8000);
//		Command cmd = new Command(Request.LOGIN, Request.CALLSITE);
//		Message msg = new Message(Key.SPEAK, "hi, fixp!");
//		Packet packet = new Packet(cmd);
//		packet.setRemote(host);
//		packet.addMessage(msg);
//		byte[] data = "测试包".getBytes();
//
//		byte[] b = packet.build(data);
//
//		System.out.printf("send packet size is:%d\n%s\n", b.length, new String(b));
//
//		FixpPacketClient client = new FixpPacketClient();
//		for (int i = 0; i < 100000; i++) {
//			System.out.printf("send num:%d\n", i + 1);
//			try {
//				client.send(packet);
//				Packet pk = client.receive();
//			} catch (IOException exp) {
//				exp.printStackTrace();
//			} catch (java.lang.Throwable exp) {
//				exp.printStackTrace();
//			}
//		}
//
//		client.close();
//		System.out.println("send packet finished!");
	}

	public void doing2() throws IOException {
		String ip = "localhost";
		int port = 9000;
		FixpStreamMonitor monitor = new FixpStreamMonitor(InetAddress.getByName(ip), port);

		boolean success = monitor.start();
		System.out.printf("fixp thread start is:%b\n", success);

		this.delay(2000);
		this.sendStream();
		this.delay(50000);

		monitor.stop();
		System.out.println("fixp stream monitor finished!");
	}

	public void sendStream() throws IOException {
		InetAddress ip = Address.select();
		int port = 9000;
		SocketHost host = new SocketHost(SocketHost.TCP, ip, port);

		Command cmd = new Command(Request.LOGIN, Request.CALLSITE);
		Message msg = new Message(Key.SPEAK, "hi, fixp!");
		Stream stream = new Stream(cmd);
		stream.setRemote(host);
		stream.addMessage(msg);
//		byte[] data = "测试包".getBytes();
		byte[] b = stream.build();
		System.out.printf("stream size is:%d\n", b.length);

		FixpStreamClient client = new FixpStreamClient();
		for (int i = 0; i < 1000; i++) {
			System.out.printf("send count:%d\n", i+1);
			try {
				// client.connect(ip, port);
				client.send(stream);
				Stream resp = client.receive(true);
			} catch (IOException exp) {
				exp.printStackTrace();
			}
		}

		this.delay(2000);
		// close socket
		client.close();
	}

	public void testPacket() {
//		Command cmd = new Command(Response.SERVER_ERROR);
//		Packet packet = new Packet(cmd);
//		packet.addMessage(new Message(Key.SPEAK, "sorry!"));
//		byte[] data = "千赶时髦江山易".getBytes();
//		byte[] b = packet.build(data);
//		System.out.printf("packet byte length:%d\n", b.length);
//
//		Packet p2 = new Packet(b);
	}

	public static void main(String[] args) {
		TestFixp fixp = new TestFixp();
//		fixp.doing2();
//		fixp.send();
//		fixp.testPacket();
	}

}