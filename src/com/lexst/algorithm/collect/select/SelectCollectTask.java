/**
 * @email admin@wigres.com
 *
 */
package com.lexst.algorithm.collect.select;

import java.util.*;

import com.lexst.algorithm.collect.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;

/**
 * 
 *
 */
public class SelectCollectTask extends CollectTask {

	/**
	 * default
	 */
	public SelectCollectTask() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.lexst.algorithm.collect.CollectTask#display(com.lexst.sql.statement.Conduct, java.util.Map, com.lexst.algorithm.collect.PrintTerminal, byte[], int, int)
	 */
	@Override
	public int display(Conduct conduct, Map<Space, Table> tables,
			PrintTerminal terminal, byte[] b, int off, int len) throws CollectTaskException {

		return 0;
	}

}
