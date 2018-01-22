/**
 * 
 */
package com.lexst.remote.client.work;

import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;

final class WorkTask {

	protected WorkClient client;

	protected ToPhase phase;

	protected NetDomain domain;

	public WorkTask(WorkClient client, ToPhase object, NetDomain domain) {
		this.client = client;
		this.phase = object;
		this.domain = domain;
	}

	public WorkClient getClient() {
		return this.client;
	}

	public ToPhase getPhase() {
		return this.phase;
	}

	public NetDomain getDomain() {
		return this.domain;
	}

}
