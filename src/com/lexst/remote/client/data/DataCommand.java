/**
 * 
 */
package com.lexst.remote.client.data;

import com.lexst.sql.conduct.*;
import com.lexst.sql.statement.*;

final class DataCommand {

	protected DataTrustor trustor;

	protected SQLMethod method;
	
	protected FromPhase phase;

	/**
	 * @param delegate
	 * @param object
	 */
	public DataCommand(DataTrustor delegate, SQLMethod object) {
		this.trustor = delegate;
		this.method = object;
	}

	public DataCommand(DataTrustor delegate, FromPhase phase) {
		this.trustor = delegate;
		this.phase = phase;
	}
}
