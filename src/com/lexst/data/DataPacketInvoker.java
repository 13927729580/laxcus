/**
 *
 */
package com.lexst.data;

import com.lexst.data.pool.*;
import com.lexst.fixp.*;
import com.lexst.fixp.monitor.*;
import com.lexst.invoke.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;

public class DataPacketInvoker implements PacketInvoker {

	private IPacketListener listener;

	/**
	 * @param listener
	 */
	public DataPacketInvoker(IPacketListener listener) {
		super();
		this.listener = listener;
	}

	/* (non-Javadoc)
	 * @see com.lexst.invoke.PacketCall#invoke(com.lexst.fixp.Packet)
	 */
	@Override
	public Packet invoke(Packet packet) {
		Packet resp = null;
		Command cmd = packet.getCommand();
		if (cmd.isRequest()) {
			resp = apply(packet);
		} else if (cmd.isResponse()) {
			resp = reply(packet);
		}
		return resp;
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
			this.shutdown(packet);
		} else if (cmd.isComeback()) {
			Launcher.getInstance().comeback();
		} else if(major == Request.DATA && minor == Request.SET_CACHE_ENTITY) {
			resp = setCacheReflex(packet);
		} else if(major == Request.DATA && minor == Request.DELETE_CACHE_ENTITY) {
			resp = deleteCacheEntity(packet);
		} else if(major == Request.DATA && minor == Request.SET_CHUNK_ENTITY) {
			resp = setChunkReflex(packet);
		}else if (major == Request.NOTIFY && minor == Request.SCANHUB) {
			// start thread, scan home site
			HubCrawler crawler = new HubCrawler();
			crawler.detect(packet);
		} else if(major == Request.NOTIFY && minor == Request.TRANSFER_HUB) {
			this.transfer(packet);
		}
		
		return resp;
	}

	/**
	 * reply a job
	 * @param packet
	 * @return
	 */
	private Packet reply(Packet packet) {
		Packet resp = null;
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
		
		return resp;
	}

	/**
	 * 接收远程节点的关闭命令
	 * 
	 * @param request
	 */
	private void shutdown(Packet request) {
		SocketHost remote = request.getRemote();
		boolean accepted = Launcher.getInstance().inShutdown(remote.getAddress());
		short code = (accepted ? Response.OKAY : Response.REFUSE);
		for (int i = 0; i < 3; i++) {
			Command cmd = new Command(code);
			Packet resp = new Packet(cmd);
			resp.addMessage(Key.SPEAK, "goodbye");
			listener.send(remote, resp);
		}
		// 停止服务
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
	
	private Packet setCacheReflex(Packet packet) {
		Packet invalid = new Packet(packet.getRemote(), new Command(Response.NOTACCEPTED));
		
		Message msg = packet.findMessage(Key.SCHEMA);
		if(msg == null) return invalid;
		String db = msg.stringValue();
		msg = packet.findMessage(Key.TABLE);
		if(msg == null) return invalid;
		String table = msg.stringValue();
		msg = packet.findMessage(Key.CHUNK_ID);
		if(msg == null) return invalid; 
		long chunkid = msg.longValue();
		byte[] data = packet.getData();
		
		// save data to local disk
		boolean success = CachePool.getInstance().setCacheReflex(db, table, chunkid, data);
		
		Command cmd = new Command(success ? Response.OKAY : Response.REFUSE);
		Packet reply = new Packet(packet.getRemote(), cmd);
		return reply;
	}
	
	private Packet deleteCacheEntity(Packet packet) {
		Packet invalid = new Packet(packet.getRemote(), new Command(Response.NOTACCEPTED));
		
		Message msg = packet.findMessage(Key.SCHEMA);
		if(msg == null) return invalid;
		String db = msg.stringValue();
		msg = packet.findMessage(Key.TABLE);
		if(msg == null) return invalid;
		String table = msg.stringValue();
		msg = packet.findMessage(Key.CHUNK_ID);
		if(msg == null) return invalid; 
		long chunkid = msg.longValue();
		
		// delete temp data from disk
		boolean success = CachePool.getInstance().deleteCacheReflex(db, table, chunkid);
		
		Command cmd = new Command(success ? Response.OKAY : Response.REFUSE);
		Packet reply = new Packet(packet.getRemote(), cmd);
		return reply;
	}
	
	private Packet setChunkReflex(Packet packet) {
		Packet invalid = new Packet(packet.getRemote(), new Command(Response.NOTACCEPTED));
		
		Message msg = packet.findMessage(Key.SCHEMA);
		if(msg == null) return invalid;
		String db = msg.stringValue();
		msg = packet.findMessage(Key.TABLE);
		if(msg == null) return invalid;
		String table = msg.stringValue();
		msg = packet.findMessage(Key.CHUNK_ID);
		if(msg == null) return invalid; 
		long chunkid = msg.longValue();
		byte[] data = packet.getData();
		
		// save data to local disk
		boolean success = ChunkPool.getInstance().setChunkReflex(db, table, chunkid, data);
		
		Command cmd = new Command(success ? Response.OKAY : Response.REFUSE);
		Packet reply = new Packet(packet.getRemote(), cmd);
		return reply;
	}
		
}