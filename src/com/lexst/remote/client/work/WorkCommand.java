/**
 * 
 */
package com.lexst.remote.client.work;

import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;

final class WorkCommand {

	protected WorkTrustor trustor;

	
	protected ToPhase phase;
	
	protected NetDomain domain;
	
	public WorkCommand(WorkTrustor delegate, ToPhase phase, NetDomain domain) {
		this.trustor = delegate;
		this.phase = phase;
		this.domain = domain;
	}

	public WorkTrustor getTrustor() {
		return this.trustor;
	}

}
