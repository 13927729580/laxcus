/**
 *
 */
package com.lexst.debug;

import java.util.*;
import com.lexst.fixp.*;
import com.lexst.fixp.client.*;
import com.lexst.fixp.monitor.*;
import com.lexst.thread.Notifier;

/**
 * @author siven
 *
 */
public class Manager {

	private static Manager selfHandle = new Manager();

//	private ArrayList<TestClient> array = new ArrayList<TestClient>();

	private Vector<TestClient> array = new Vector<TestClient>();

	/**
	 *
	 */
	private Manager() {
		super();
	}

	public static Manager getInstance() {
		return Manager.selfHandle;
	}

	public synchronized boolean remove(TestClient client) {
//		int size = array.size();
//		System.out.printf("REMOVE MANAGER OBJECT! size is:%d\n", size);
//		for(int i = 0; i<size; i++) {
//			TestClient tc = array.get(i);
//			if(tc.getIndex() == client.getIndex()) {
//				TestClient obj = array.remove(i);
//				return obj != null;
//			}
//		}
//		return false;

		System.out.println("REMOVE MANAGER OBJECT! size is:%d");
		return array.remove(client);
	}

//	public synchronized boolean remove(int value) {
//		System.out.println("REMOVE MANAGER OBJECT!");
//		return all.remove(new Integer(value));
//	}

	public synchronized void delay(long timeout) {
		try {
			this.wait(timeout);
		} catch (java.lang.InterruptedException exp) {

		}
	}

	public void doing() {
		RawPacketCall rawImpl = new RawPacketCall();
		FixpPacketMonitor monitor = new FixpPacketMonitor();
		monitor.setPacketCall(rawImpl);
		monitor.setPrint(false);

		int num = 20;
		for (int i = 0; i < num; i++) {
			TestClient client = new TestClient(i+1);
			this.array.add(client);
		}

		monitor.start();

//		System.out.printf("all client size is:%d\n", array.size());
//		for (TestClient client : this.array) {
//			client.start();
//			this.delay(100);
//		}

		while (!array.isEmpty()) {
			this.delay(2000);
			System.out.printf("client size is:%d\n", array.size());
		}

		System.out.println("STOP FIXP MONITOR!");

		Notifier notify = new Notifier();
		monitor.stop(notify);

		while(!notify.isKnown()) {
			this.delay(500);
		}
		System.out.println("ALL FINISHED!");
	}

	public static void main(String[] args) {
//		Manager m = new Manager();
//		m.doing();

		Manager.getInstance().doing();
	}
}
