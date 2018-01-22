/**
 *
 */
package com.lexst.call;


import com.lexst.data.Launcher;
import com.lexst.fixp.*;
import com.lexst.fixp.monitor.*;
import com.lexst.invoke.*;
import com.lexst.pool.site.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;


public class CallPacketInvoker implements PacketInvoker {

	private IPacketListener listener;

	private CallLauncher callInstance;

	/**
	 * default
	 */
	public CallPacketInvoker(CallLauncher instance, IPacketListener reply) {
		this.callInstance = instance;
		this.listener = reply;
	}

	/* (non-Javadoc)
	 * @see com.lexst.invoke.PacketCall#invoke(com.lexst.fixp.Packet)
	 */
	@Override
	public Packet invoke(Packet packet) {
		Packet reply = null;
		Command cmd = packet.getCommand();
		if (cmd.isRequest()) {
			reply = apply(packet);
		} else if (cmd.isResponse()) {
			reply = reply(packet);
		}
		return reply;
	}

	/**
	 * apply a job
	 * @param packet
	 * @return
	 */
	private Packet apply(Packet packet) {
		Packet resp = null;
		Command cmd = packet.getCommand();
				
		byte major = cmd.getMajor();
		byte minor = cmd.getMinor();

		if (cmd.isShutdown()) {
			shutdown(packet);
		} else if(cmd.isComeback()) {
			callInstance.comeback();	
		} else if(major == Request.NOTIFY && minor == Request.REFRESH_DATASITE) {
//			DataPool.getInstance().refresh();
			FromPool.getInstance().refresh();
		} else if(major == Request.NOTIFY && minor == Request.REFRESH_WORKSITE) {
//			WorkPool.getInstance().refresh();
			ToPool.getInstance().refresh();
		} else if (major == Request.NOTIFY && minor == Request.SCANHUB) {
			// start thread, scan home site
			HubCrawler crawler = new HubCrawler();
			crawler.detect(packet);
		} else if(major == Request.NOTIFY && minor == Request.TRANSFER_HUB) {
			this.transfer(packet);
		} else if (major == Request.NOTIFY && minor == Request.OVERLOAD) {
//			String msg = packet.findChar(Key.LOCAL_ADDRESS);
//			SiteHost host = new SiteHost(msg);
		}

		return resp;
	}

	/**
	 * reply a job
	 * @param reply
	 * @return
	 */
	private Packet reply(Packet reply) {
		Packet resp = null;
		Command cmd = reply.getCommand();
		short code = cmd.getResponse();

		switch (code) {
		case Response.ISEE:
			callInstance.refreshEndTime();
			break;
		case Response.NOTLOGIN:
			Launcher.getInstance().setOperate(BasicLauncher.LOGIN);
			break;
		}

		return resp;
	}

	/**
	 * 接受/拒绝远程节点的关闭请求，判断依据是远程节点的地址是否在本地关闭表配置中
	 * 
	 * @param request
	 */
	private void shutdown(Packet request) {
		SocketHost remote = request.getRemote();
		boolean accepted = callInstance.inShutdown(remote.getAddress());
		short code = (accepted ? Response.OKAY : Response.REFUSE);
		for (int i = 0; i < 3; i++) {
			Command cmd = new Command(code);
			Packet resp = new Packet(cmd);
			resp.addMessage(Key.SPEAK, "goodbye!");
			listener.send(remote, resp);
		}
		// 关闭CALL节点服务
		if (accepted) {
			callInstance.stop();
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
}