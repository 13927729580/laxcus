/**
 *
 */
package com.lexst.debug;

import com.lexst.fixp.*;
import com.lexst.invoke.PacketInvoker;

/**
 * @author siven
 *
 */
public class RawPacketCall implements PacketInvoker {

	/**
	 *
	 */
	public RawPacketCall() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.lexst.invoke.PacketCall#invoke(com.lexst.fixp.Packet)
	 */
	@Override
	public Packet invoke(Packet requst) {
//		System.out.println("RAW PACKET CALL");

		Command cmd = new Command(Response.ISEE);
		Packet reply = new Packet(cmd);
		reply.addMessage(new Message(Key.IP, "Pentium"));
		return reply;
	}

}
