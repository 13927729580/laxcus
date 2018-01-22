/**
 *
 */
package com.lexst.shutdown;

import java.io.*;
import java.net.*;

import org.w3c.dom.*;

import com.lexst.fixp.*;
import com.lexst.fixp.client.*;
import com.lexst.util.host.*;
import com.lexst.xml.*;

/**
 *
 * 发起远程停止命令
 *
 */
public class Shutdown {

	/**
	 * default
	 */
	public Shutdown() {
		super();
	}

	/**
	 * 从文件中提取本地地址，发送停止包到本地节点
	 * @param filename
	 * @return
	 */
	public boolean send(String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) return false;

		Element elem = (Element) document.getElementsByTagName("local-site").item(0);
		String ip = xml.getXMLValue(elem.getElementsByTagName("ip"));
		String udport = xml.getXMLValue(elem.getElementsByTagName("udp-port"));
		
		SocketHost remote = null;
		try {
			InetAddress inet = InetAddress.getByName(ip);
			remote = new SocketHost(SocketHost.UDP, inet, Integer.parseInt(udport));
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return false;
		}

		Command cmd = new Command(Request.NOTIFY, Request.SHUTDOWN);
		Packet request = new Packet(cmd);
		request.setRemote(remote);

		boolean success = false;
		FixpPacketClient client = new FixpPacketClient();
		// 设置数据包接收超时时间
		client.setReceiveTimeout(10);
		// 发送数据包，收回返回的数据包
		for (int i = 0; i < 3; i++) {
			try {
				Packet resp = client.execute(request);
				cmd = resp.getCommand();
				if (cmd.getResponse() == Response.OKAY) {
					success = true;
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();
				client.close();
			}
		}

		SocketHost address = client.getLocal();
		if(success) {
			System.out.printf("%s send command to %s\n", (address == null ? "local" : address), remote);
		} else {
			System.out.printf("%s cannot send command to %s\n", (address == null ? "local" : address), remote);
		}

		client.close();
		return success;
	}

	public static void main(String[] args) {
		Shutdown shutdown = new Shutdown();
		if (args.length == 1) {
			String filename = args[0];
			shutdown.send(filename);
		} else {
			System.out.println("invalid!");
		}
	}

}