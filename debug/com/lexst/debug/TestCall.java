/**
 *
 */
package com.lexst.debug;

import com.lexst.fixp.*;

/**
 * @author siven
 *
 */
public class TestCall {

	/**
	 *
	 */
	public TestCall() {
		// TODO Auto-generated constructor stub
	}

	public void test() {
		String filename = "D:/workspace/lexst/src/com/lexst/call/local.xml";
		//com.lexst.call.Launcher.getInstance().loadXML(filename);
	}

	public void test2() {
		byte[] data = "PENTIUM PRO YSTEM".getBytes();
		Command cmd = new Command(Request.RPC, Request.EXECUTE);
		Stream request = new Stream(cmd);
		// apply.addMessage(new com.lexst.fixp.Message(com.lexst.fixp.Key.RQUEST_TYPE, com.lexst.fixp.Value.RCP_REQUEST));
		request.setData(data);

		byte[] b = request.build();

		System.out.printf("request byte is:%d\n", b.length);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TestCall call = new TestCall();
//		call.test();
		call.test2();
	}

}
