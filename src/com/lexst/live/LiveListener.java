/**
 * 
 */
package com.lexst.live;

import com.lexst.util.host.SocketHost;

public interface LiveListener {

	void flicker();

	void shutdown();

	void disconnect();

	void active(int num, SocketHost topsite);
}