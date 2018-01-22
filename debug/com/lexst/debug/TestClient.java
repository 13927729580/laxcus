/**
 *
 */
package com.lexst.debug;

import java.io.IOException;
import java.net.*;

import com.lexst.thread.VirtualThread;
import com.lexst.util.host.*;

import com.lexst.fixp.*;
import com.lexst.fixp.client.*;
import com.lexst.fixp.monitor.*;

/**
 * @author siven
 *
 */
public class TestClient extends VirtualThread {

	private int index = -1;

	/**
	 *
	 */
	public TestClient(int index) {
		// TODO Auto-generated constructor stub
		this.index = index;
	}

	public int getIndex() {
		return index;
	}

	public boolean equals(Object obj) {
		if(obj == this) return true;

		System.out.println("THIS IS equals method!");
		TestClient client = (TestClient)obj;
		return index == client.index;
	}

	public int hashCode() {
		return index;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		System.out.println("In TestClient finished method!");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// TODO Auto-generated method stub
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		this.delay(2000);
		this.subprocess();
		this.stop();
		System.out.println("client exit!");

		boolean b = Manager.getInstance().remove(this);
		System.out.printf("Client finished, remove is:%b\n", b);

		this.delay(2000);
	}


	public void subprocess() {
		InetAddress localIP = Address.select(); 
		SocketHost host = new SocketHost(SocketHost.UDP, localIP, FixpPacketMonitor.FIXP_PACKET_PORT);
		System.out.printf("SEND TO:%s\n", host);

		byte[] data = new byte[128];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) 'a';
		}
		Command cmd = new Command(Request.LOGIN, Request.CALLSITE);
		Packet request = new Packet(cmd);
		request.addMessage(new Message(Key.IP, "PENTIUM"));
		request.setData(data);
		request.setRemote(host);

		FixpPacketClient client = new FixpPacketClient();

		int num = 50;
		for (int i = 0; i < num; i++) {
			try {
				Packet resp = client.execute(request);
				if (resp == null) {
					System.out.println("this is null");
				}
			} catch (IOException exp) {
				exp.printStackTrace();
				client.close();
			}
			System.out.printf("%d exchange num:%d\n", this.index, i + 1);
		}

		client.close();
	}

}