/**
 *
 */
package com.lexst.visit.impl.top;

import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.site.*;
import com.lexst.sql.account.*;
import com.lexst.sql.schema.*;
import com.lexst.top.*;
import com.lexst.top.pool.*;
import com.lexst.util.datetime.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.visit.naming.top.*;

public class TopVisitImpl implements TopVisit {

	/**
	 * default
	 */
	public TopVisitImpl() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.Visit#nothing()
	 */
	@Override
	public void nothing() throws VisitException {
		Launcher.getInstance().nothing();
	}

	/*
	 * get server system time
	 * @see com.lexst.visit.Visit#currentTime()
	 */
	public long currentTime() throws VisitException {
		return SystemTime.get();
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#applyChunkId(int)
	 */
	@Override
	public long[] applyChunkId(int num) throws VisitException {
		if (Launcher.getInstance().isRunsite()) {
			return Launcher.getInstance().pullSingle(num);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#pullKey(java.lang.String, java.lang.String, int)
	 */
	@Override
	public Number[] pullKey(String schema, String table, int size) throws VisitException {
		if (Launcher.getInstance().isRunsite()) {
			return Launcher.getInstance().pullKey(schema, table, size);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#findChunkSize(java.lang.String, java.lang.String)
	 */
	@Override
	public int findChunkSize(String schema, String table) throws VisitException {
		return Launcher.getInstance().findChunkSize(schema, table);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#findTable(java.lang.String, java.lang.String)
	 */
	@Override
	public Table findTable(SiteHost host, String schema, String table) throws VisitException {
		if (LivePool.getInstance().exists(host) || HomePool.getInstance().exists(host)) {
			return Launcher.getInstance().findTable(schema, table);
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#getSiteTimeout(int)
	 */
	@Override
	public int getSiteTimeout(int type) throws VisitException {
		int timeout = 20;
		switch (type) {
		case Site.HOME_SITE:
			timeout = HomePool.getInstance().getSiteTimeout() - 2;
			break;
		case Site.LIVE_SITE:
			timeout = LivePool.getInstance().getSiteTimeout() - 2;
			break;
		}
		return timeout;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#hello(com.lexst.util.host.SiteHost)
	 */
	@Override
	public int hello(int type, SiteHost host) throws VisitException {
		int status = 0;
		if (type == Site.HOME_SITE) {
			status = HomePool.getInstance().refresh(host);
		} else if(type == Site.LIVE_SITE) {
			status = LivePool.getInstance().refresh(host);
		}
		return status;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#login(com.lexst.site.Site)
	 */
	@Override
	public boolean login(Site site) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			if (site.isHome()) {
				code = HomePool.getInstance().add(site);
			} else if (site.isLive()) {
				code = LivePool.getInstance().add(site);
			}
		}
		return code == Response.ACCEPTED;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#logout(int, com.lexst.util.host.SiteHost)
	 */
	@Override
	public boolean logout(int type, SiteHost host) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			if (type == Site.HOME_SITE) {
				code = HomePool.getInstance().remove(host);
			} else if (type == Site.LIVE_SITE) {
				code = LivePool.getInstance().remove(host);
			}
		}
		return code == Response.ACCEPTED;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#relogin(com.lexst.site.Site)
	 */
	@Override
	public boolean relogin(Site site) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			if (site.isHome()) {
				code = HomePool.getInstance().update(site);
			} else if (site.isLive()) {
				code = LivePool.getInstance().update(site);
			}
		}
		return code == Response.ACCEPTED;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#findHomeSite(java.lang.String, java.lang.String)
	 */
	@Override
	public SiteHost[] findHomeSite(String schema, String table) throws VisitException {
		return HomePool.getInstance().find(schema, table);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#setChunkSize(java.lang.String, java.lang.String, int)
	 */
	@Override
	public boolean setChunkSize(String schema, String table, int size) throws VisitException {
		Space space = new Space(schema, table);
		if (Launcher.getInstance().isRunsite()) {
			return Launcher.getInstance().setChunkSize(space, size);
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#selectCallSite(java.lang.String)
	 */
	@Override
	public SiteHost[] selectCallSite(String naming) throws VisitException {
		return HomePool.getInstance().selectCallSite(naming);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#selectCallSite(java.lang.String, java.lang.String)
	 */
	@Override
	public SiteHost[] selectCallSite(String schema, String table) throws VisitException {
		return HomePool.getInstance().selectCallSite(new Space(schema, table));
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#selectCallSite(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public SiteHost[] selectCallSite(String naming, String db, String table) throws VisitException {
		return HomePool.getInstance().selectCallSite(naming, new Space(db, table));
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#createSchema(com.lexst.util.host.SiteHost, com.lexst.sql.schema.Schema)
	 */
	@Override
	public boolean createSchema(SiteHost local, Schema schema) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			code = LivePool.getInstance().createSchema(local, schema);
		}
		return code == Response.ACCEPTED;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#showSchema(com.lexst.util.host.SiteHost, java.lang.String)
	 */
	@Override
	public Schema findSchema(SiteHost local, String schema) throws VisitException {
		if (Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return Launcher.getInstance().showSchema(schema);
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#deleteSchema(com.lexst.util.host.SiteHost, java.lang.String)
	 */
	@Override
	public boolean deleteSchema(SiteHost local, String db) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			code = LivePool.getInstance().deleteSchema(local, db);
		}
		return code == Response.ACCEPTED;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#getSchemas(com.lexst.util.host.SiteHost)
	 */
	@Override
	public String[] getSchemas(SiteHost local) throws VisitException {
		return LivePool.getInstance().getSchemas(local);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#createUser(com.lexst.util.host.SiteHost, com.lexst.sql.account.User)
	 */
	@Override
	public boolean createUser(SiteHost local, User user) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			code = LivePool.getInstance().createUser(local, user);
		}
		return code == Response.ACCEPTED;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#deleteUser(com.lexst.util.host.SiteHost, java.lang.String)
	 */
	@Override
	public boolean deleteUser(SiteHost local, String username) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			code = LivePool.getInstance().deleteUser(local, username);
		}
		return code == Response.ACCEPTED;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#alterUser(com.lexst.util.host.SiteHost, com.lexst.sql.account.User)
	 */
	@Override
	public boolean alterUser(SiteHost local, User user) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			code = LivePool.getInstance().alterUser(local, user);
		}
		return code == Response.ACCEPTED;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#onUser(com.lexst.util.host.SiteHost, java.lang.String)
	 */
	@Override
	public boolean onUser(SiteHost local, String username) throws VisitException {
		if (Launcher.getInstance().isRunsite()) {
			return LivePool.getInstance().onUser(local, username);
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#addPermit(com.lexst.util.host.SiteHost, com.lexst.sql.account.Permit)
	 */
	@Override
	public boolean addPermit(SiteHost local, Permit permit) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			code = LivePool.getInstance().addPermit(local, permit);
		}
		Logger.debug("TopVisitImple.addPermit, result code %d", code);
		return code == Response.ACCEPTED;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#deletePermit(com.lexst.util.host.SiteHost, com.lexst.sql.account.Permit)
	 */
	@Override
	public boolean deletePermit(SiteHost local, Permit permit) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			code = LivePool.getInstance().deletePermit(local, permit);
		}
		return code == Response.ACCEPTED;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#getPermits(com.lexst.util.host.SiteHost)
	 */
	@Override
	public Permit[] getPermits(SiteHost local) throws VisitException {
		return LivePool.getInstance().getPermits(local);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#createTable(com.lexst.util.host.SiteHost, com.lexst.sql.schema.Table)
	 */
	@Override
	public boolean createTable(SiteHost local, Table table) throws VisitException {
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			code = LivePool.getInstance().createTable(local, table);
		}
		return code == Response.ACCEPTED;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#deleteTable(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String)
	 */
	@Override
	public boolean deleteTable(SiteHost local, String schema, String table) throws VisitException {
		Space space = new Space(schema, table);
		short code = 0;
		if (Launcher.getInstance().isRunsite()) {
			code = LivePool.getInstance().deleteTable(local, space);
		}
		return code == Response.ACCEPTED;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#getTables(com.lexst.util.host.SiteHost)
	 */
	@Override
	public Table[] getTables(SiteHost local) throws VisitException {
		return LivePool.getInstance().getTables(local);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#checkIdentified(com.lexst.util.host.SiteHost)
	 */
	@Override
	public short checkIdentified(SiteHost local) throws VisitException {
		return LivePool.getInstance().checkIdentified(local);
	}

	/*
	 * 定义表的数据重构时间
	 * @see com.lexst.visit.naming.top.TopVisit#setRebuildTime(java.lang.String, java.lang.String, short, int, long)
	 */
	@Override
	public boolean setRebuildTime(String schema, String table, short columnId, int type, long time) throws VisitException {
		Space space = new Space(schema, table);
		if (Launcher.getInstance().isRunsite()) {
			return Launcher.getInstance().setRebuildTime(space, columnId, type, time);
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#rebuild(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, short, com.lexst.util.host.Address[])
	 */
	@Override
	public Address[] rebuild(SiteHost local, String schema, String table, short columnId, Address[] sites) throws VisitException {
		Space space = new Space(schema, table);
		if (Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return HomePool.getInstance().rebuild(space, columnId, sites);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#loadIndex(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, com.lexst.util.host.Address[])
	 */
	@Override
	public Address[] loadIndex(SiteHost local, String schema, String table, Address[] sites) throws VisitException {
		Space space = new Space(schema, table);
		if(Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return HomePool.getInstance().loadIndex(space, sites);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#stopIndex(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, com.lexst.util.host.Address[])
	 */
	@Override
	public Address[] stopIndex(SiteHost local, String schema, String table, Address[] sites) throws VisitException {
		Space space = new Space(schema, table);
		if (Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return HomePool.getInstance().stopIndex(space, sites);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#loadChunk(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, com.lexst.util.host.Address[])
	 */
	@Override
	public Address[] loadChunk(SiteHost local, String schema, String table, Address[] sites) throws VisitException {
		Space space = new Space(schema, table);
		if (Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return HomePool.getInstance().loadChunk(space, sites);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#stopChunk(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, com.lexst.util.host.Address[])
	 */
	@Override
	public Address[] stopChunk(SiteHost local, String schema, String table, Address[] sites) throws VisitException {
		Space space = new Space(schema, table);
		if (Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return HomePool.getInstance().stopChunk(space, sites);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#buildTask(com.lexst.util.host.SiteHost, java.lang.String, com.lexst.util.host.Address[])
	 */
	@Override
	public Address[] buildTask(SiteHost local, String naming, Address[] hosts) throws VisitException {
		if (Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return HomePool.getInstance().buildTask(naming, hosts);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#showChunkSize(com.lexst.util.host.SiteHost, java.lang.String, int, com.lexst.util.host.Address[])
	 */
	@Override
	public long showChunkSize(SiteHost local, String schema, int siteType,
			Address[] sites) throws VisitException {
		if (Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return HomePool.getInstance().showChunkSize(schema, siteType, sites);	
		}
		return -1L;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#showChunkSize(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, int, com.lexst.util.host.Address[])
	 */
	@Override
	public long showChunkSize(SiteHost local, String schema, String table,
			int siteType, Address[] sites) throws VisitException {
		if (Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return HomePool.getInstance().showChunkSize(schema, table, siteType, sites);
		}
		return -1L;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#showTask(com.lexst.util.host.SiteHost, java.lang.String, com.lexst.util.host.Address[])
	 */
	@Override
	public String showTask(SiteHost local, String tag, Address[] sites) throws VisitException {
		if (Launcher.getInstance().isRunsite() && LivePool.getInstance().exists(local)) {
			return HomePool.getInstance().showTask(tag, sites);
		}
		return "";
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.top.TopVisit#showSite(int, com.lexst.util.host.Address[])
	 */
	@Override
	public SiteHost[] showSite(int site, Address[] froms) throws VisitException {
		return Launcher.getInstance().showSite(site, froms);
	}
}