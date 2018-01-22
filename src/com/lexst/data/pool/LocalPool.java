/**
 * 
 */
package com.lexst.data.pool;

import java.io.*;

import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.remote.client.data.*;
import com.lexst.util.host.*;


abstract class LocalPool extends JobPool {

	/**
	 * 
	 */
	protected LocalPool() {
		super();
	}
	
	/**
	 * return a data client handle
	 * @return
	 */
	protected DataClient apply(SiteHost data) {
		SocketHost address = data.getStreamHost();
		DataClient client = new DataClient(true, address);
		for (int i = 0; i < 3; i++) {
			try {
				client.reconnect();
				return client;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(1000);
		}
		return null;
	}
	
	/**
	 * @param client
	 */
	protected void complete(DataClient client) {
		if(client == null) return;
		try {
			client.exit();
			client.close();
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
	}
	
	
	protected String buildChunkFile(String path, long chunkid) {
		File dir = new File(path);
		if (!dir.exists() || !dir.isDirectory()) {
			dir.mkdirs();
		}
		// 数据块文件名
		String s = String.format("%x", chunkid);
		while (s.length() < 16) {
			s = "0" + s;
		}
		s = String.format("%s.lxdb", s);

		return new File(dir, s).getAbsolutePath();
	}
}