/**
 * 
 */
package com.lexst.remote.client.data;

import com.lexst.sql.conduct.*;
import com.lexst.sql.statement.*;

final class DataTask {

	protected DataClient client;

	protected SQLMethod method;
	
	protected FromPhase phase;

	/**
	 * @param client
	 * @param object
	 */
	public DataTask(DataClient client, SQLMethod object) {
		this.client = client;
		this.method = object;
	}

	/**
	 * @param client
	 * @param method
	 */
	public DataTask(DataClient client, FromPhase phase) {
		this.client = client;
		this.phase = phase;
	}
	
	public void release() {
		client = null;
		method = null;
		phase = null;
	}
}