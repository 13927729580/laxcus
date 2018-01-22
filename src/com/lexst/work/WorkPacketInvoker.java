/**
 *
 */
package com.lexst.work;

import com.lexst.fixp.*;
import com.lexst.fixp.monitor.*;
import com.lexst.invoke.*;
import com.lexst.log.client.*;
import com.lexst.pool.site.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;
import com.lexst.work.pool.*;

/**
 * WORK节点数据包调用分派器。<br>
 *
 */
public class WorkPacketInvoker implements PacketInvoker {

	private IPacketListener listener;
	
	/**
	 * @param reply
	 */
	public WorkPacketInvoker(IPacketListener reply) {
		super();
		this.listener = reply;
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.invoke.PacketInvoker#invoke(com.lexst.fixp.Packet)
	 */
	@Override
	public Packet invoke(Packet packet) {
		Command cmd = packet.getCommand();
		if (cmd.isRequest()) {
			return this.apply(packet);
		} else if (cmd.isResponse()) {
			return this.reply(packet);
		}
		return null;
	}

	private Packet apply(Packet packet) {
		Packet resp = null;
		Command cmd = packet.getCommand();
		byte major = cmd.getMajor();
		byte minor = cmd.getMinor();

		if (cmd.isShutdown()) {
			Logger.debug("WorkPacketInvoker. shutdown work site!");
			shutdown(packet);
		} else if (cmd.isComeback()) {
			Launcher.getInstance().comeback();
		} else if(major == Request.NOTIFY && minor == Request.REFRESH_DATASITE) {
			FromPool.getInstance().refresh(); // 更新DATA节点记录
		} else if (major == Request.SQL && minor == Request.SQL_CONDUCT) {
			resp = this.conduct(packet);
		} else if (major == Request.NOTIFY && minor == Request.SCANHUB) {
			// start thread, scan home site
			HubCrawler crawler = new HubCrawler();
			crawler.detect(packet);
		} else if(major == Request.NOTIFY && minor == Request.TRANSFER_HUB) {
			this.transfer(packet);
		}

		return resp;
	}
	
	/**
	 * @param packet
	 * @return
	 */
	private Packet reply(Packet packet) {
		Packet reply = null;
		Command cmd = packet.getCommand();
		short code = cmd.getResponse();
		
		switch (code) {
		case Response.ISEE:
			Launcher.getInstance().refreshEndTime();
			break;
		case Response.NOTLOGIN:
			Launcher.getInstance().setOperate(BasicLauncher.LOGIN);
			break;
		}
		
		return reply;
	}

	/**
	 * 接受远程退出命令
	 * @param request
	 */
	private void shutdown(Packet request) {
		SocketHost remote = request.getRemote();
		// 检查地址是否在接受范围
		boolean accepted = Launcher.getInstance().inShutdown(remote.getAddress());
		// 如果远程地址在关闭表范围内就接受，否则拒绝
		short code = (accepted ? Response.OKAY : Response.REFUSE);
		for (int i = 0; i < 3; i++) {
			Command cmd = new Command(code);
			Packet resp = new Packet(cmd);
			resp.addMessage(Key.SPEAK, "goodbye!");
			// 发送返回应答包
			listener.send(remote, resp);
		}
		// 停止WORK节点服务，退出JVM
		if (accepted) {
			Launcher.getInstance().stop();
		}
	}
	
	private void transfer(Packet packet) {
		String server_address = packet.findChar(Key.LOCAL_ADDRESS);
		if (server_address == null)	return;

		try {
			SiteHost host = new SiteHost(server_address);
			Launcher.getInstance().setHubSite(host);
			Launcher.getInstance().setOperate(BasicLauncher.LOGIN);
		} catch (Throwable exp) {
			return;
		}
	}

	/**
	 * 执行数据包AGGREGATE阶段分布计算，AGGREGATE阶段允许有多个子阶段
	 * @param request
	 * @return
	 */
	private Packet conduct(Packet request) {
		SocketHost remote = request.getRemote();
		byte[] data = request.getData();
		Packet resp = (Packet) ConductPool.getInstance().conduct(data, 0, data.length, false);
		resp.setRemote(remote);
		return resp;
	}
}