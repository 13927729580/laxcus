/**
 * 
 */
package com.lexst.visit.impl.work;

import com.lexst.algorithm.disk.*;
import com.lexst.visit.*;
import com.lexst.visit.naming.work.*;
import com.lexst.work.*;

/**
 * WorkVisit实现类
 *
 */
public class WorkVisitImpl implements WorkVisit {

	/**
	 * default constructor
	 */
	public WorkVisitImpl() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.Visit#nothing()
	 */
	@Override
	public void nothing() throws VisitException {
		Launcher.getInstance().nothing();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.work.WorkVisit#suckup(long, int, long, long)
	 */
	@Override
	public byte[] suckup(long jobid, int mod, long begin, long end) throws VisitException {
		return DiskPool.getInstance().read(jobid, mod, begin, end);
	}

//	/* (non-Javadoc)
//	 * @see com.lexst.visit.naming.work.WorkVisit#getRefreshTime()
//	 */
//	@Override
//	public int getRefreshTime() throws VisitException {
//		return DataPool.getInstance().getSiteTimeout() - 2;
//	}

//	/* (non-Javadoc)
//	 * @see com.lexst.visit.naming.work.WorkVisit#login(com.lexst.util.host.SocketHost, com.lexst.util.host.SiteHost)
//	 */
//	@Override
//	public boolean login(SocketHost local, SiteHost monitor) throws VisitException {
//		return DataPool.getInstance().add(local, monitor) == Response.ACCEPTED;
//	}
//
//	/* (non-Javadoc)
//	 * @see com.lexst.visit.naming.work.WorkVisit#logout(com.lexst.util.host.SocketHost)
//	 */
//	@Override
//	public boolean logout(SocketHost local) throws VisitException {
//		return DataPool.getInstance().remove(local) == Response.ACCEPTED;
//	}
//
//	/* (non-Javadoc)
//	 * @see com.lexst.visit.naming.work.WorkVisit#relogin(com.lexst.util.host.SocketHost, com.lexst.util.host.SiteHost)
//	 */
//	@Override
//	public boolean relogin(SocketHost local, SiteHost monitor) throws VisitException {
//		return DataPool.getInstance().update(local, monitor) == Response.ACCEPTED;
//	}
//
//	/* (non-Javadoc)
//	 * @see com.lexst.visit.naming.work.WorkVisit#refresh(com.lexst.util.host.SocketHost)
//	 */
//	@Override
//	public boolean refresh(SocketHost local) throws VisitException {
//		return DataPool.getInstance().refresh(local) == Response.ISEE;
//	}



}