/**
 *
 */
package com.lexst.visit.impl.home;

import java.util.List;
import java.util.ArrayList;

import com.lexst.home.*;
import com.lexst.home.pool.*;
import com.lexst.site.*;
import com.lexst.sql.schema.*;
import com.lexst.util.datetime.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.visit.naming.home.*;

public class HomeVisitImpl implements HomeVisit {

	/**
	 * default
	 */
	public HomeVisitImpl() {
		super();
	}

	/*
	 * get server system time
	 * @see com.lexst.visit.Visit#currentTime()
	 */
	public long currentTime() throws VisitException {
		return SystemTime.get();
	}
	
	/*
	 * get site timeout
	 * @see com.lexst.visit.naming.home.HomeVisit#getTimeout(int)
	 */
	public int getSiteTimeout(int family) throws VisitException {
		int second = 20; // default is 20 second
		switch (family) {
		case Site.LOG_SITE:
			second = LogPool.getInstance().getSiteTimeout();
			break;
		case Site.DATA_SITE:
			second = DataPool.getInstance().getSiteTimeout();
			break;
		case Site.CALL_SITE:
			second = DataPool.getInstance().getSiteTimeout();
			break;
		case Site.WORK_SITE:
			second = WorkPool.getInstance().getSiteTimeout();
			break;
		case Site.BUILD_SITE:
			second = BuildPool.getInstance().getSiteTimeout();
			break;
		}
		second -= 2;
		if (second < 10) second = 10;
		return second;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#batchWorkSite()
	 */
	@Override
	public Site[] batchWorkSite() throws VisitException {
		return WorkPool.getInstance().batch();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#batchDataSite()
	 */
	@Override
	public Site[] batchDataSite() throws VisitException {
		return DataPool.getInstance().batch();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#applyLogSite()
	 */
	@Override
	public SiteHost findLogSite(int siteType) throws VisitException {
		return LogPool.getInstance().find(siteType);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findDatSite(java.lang.String, java.lang.String)
	 */
	@Override
	public SiteHost[] findDataSite(String db, String table) throws VisitException {
		return DataPool.getInstance().findSite(new Space(db, table));
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findDataSite(java.lang.String, java.lang.String, int)
	 */
	@Override
	public SiteHost[] findDataSite(String db, String table, int rank) throws VisitException {
		return DataPool.getInstance().findSite(new Space(db, table), rank);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findDataSite(java.lang.String, java.lang.String, long)
	 */
	@Override
	public SiteHost[] findDataSite(String db, String table, long chunkid) throws VisitException {
		return DataPool.getInstance().findSite(new Space(db, table), chunkid);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findCallSite(java.lang.String, java.lang.String)
	 */
	@Override
	public SiteHost[] findCallSite(String db, String table) throws VisitException {
		return CallPool.getInstance().findSite(new Space(db, table));
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findCallSite(java.lang.String)
	 */
	@Override
	public SiteHost[] findCallSite(String naming) throws VisitException {
		return CallPool.getInstance().findSite(naming);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findCallSite(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public SiteHost[] findCallSite(String naming, String db, String table) throws VisitException {
		return CallPool.getInstance().findSite(naming, new Space(db, table));
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findWorkSite(java.lang.String)
	 */
	@Override
	public SiteHost[] findWorkSite(String naming) throws VisitException {
		return WorkPool.getInstance().find(naming);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findBuildSite(java.lang.String, java.lang.String)
	 */
	public SiteHost[] findBuildSite(String db, String table) throws VisitException {
		return BuildPool.getInstance().find(new Space(db, table));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.visit.naming.home.HomeVisit#findBuildSite(java.lang.String)
	 */
	public SiteHost[] findBuildSite(String naming) throws VisitException {
		return BuildPool.getInstance().find(naming);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.visit.naming.home.HomeVisit#findBuildSite()
	 */
	public SiteHost[] findBuildSite() throws VisitException {
		return BuildPool.getInstance().getHosts();
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.lead.LeadVisit#applyChunkId(int)
	 */
	@Override
	public long[] pullSingle(int num) throws VisitException {
		return Launcher.getInstance().pullSingle(num);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#pullKey(java.lang.String, java.lang.String, int)
	 */
	@Override
	public Number[] pullKey(String db, String table, int num) throws VisitException {
		return Launcher.getInstance().pullKey(db, table, num);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#balance(int)
	 */
	@Override
	public Space[] balance(int num) throws VisitException {
		return Launcher.getInstance().balance(num);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findChunkSize(java.lang.String, java.lang.String)
	 */
	@Override
	public int findChunkSize(String db, String table) throws VisitException {
		return Launcher.getInstance().findChunkSize(db, table);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.Visit#nothing()
	 */
	@Override
	public void nothing() throws VisitException {
		Launcher.getInstance().nothing();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#hello(int, java.lang.String, int, int)
	 */
	@Override
	public int hello(int siteType, SiteHost host) throws VisitException {
		int status = 0;
		switch (siteType) {
		case Site.LOG_SITE:
			status = LogPool.getInstance().refresh(host);
			break;
		case Site.DATA_SITE:
			status = DataPool.getInstance().refresh(host);
			break;
		case Site.CALL_SITE:
			status = CallPool.getInstance().refresh(host);
			break;
		case Site.WORK_SITE:
			status = WorkPool.getInstance().refresh(host);
			break;
		case Site.BUILD_SITE:
			status = BuildPool.getInstance().refresh(host);
			break;
		}
		return status;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#login(com.lexst.site.Site)
	 */
	@Override
	public boolean login(Site site) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return false;
		}
		boolean success = false;
		switch (site.getFamily()) {
		case Site.LOG_SITE:
			success = LogPool.getInstance().add(site);
			break;
		case Site.DATA_SITE:
			success = DataPool.getInstance().add(site);
			break;
		case Site.CALL_SITE:
			success = CallPool.getInstance().add(site);
			break;
		case Site.WORK_SITE:
			success = WorkPool.getInstance().add(site);
			break;
		case Site.BUILD_SITE:
			success = BuildPool.getInstance().add(site);
			break;
		}
		return success;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#logout(int, com.lexst.util.host.SiteHost)
	 */
	@Override
	public boolean logout(int type, SiteHost host) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return false;
		}
		boolean success = false;
		switch(type) {
		case Site.LOG_SITE:
			success = LogPool.getInstance().remove(host);
			break;
		case Site.DATA_SITE:
			success = DataPool.getInstance().remove(host);
			break;
		case Site.CALL_SITE:
			success = CallPool.getInstance().remove(host);
			break;
		case Site.WORK_SITE:
			success = WorkPool.getInstance().remove(host);
			break;
		case Site.BUILD_SITE:
			success = BuildPool.getInstance().remove(host);
			break;
		}
		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#relogin(com.lexst.site.Site)
	 */
	@Override
	public boolean relogin(Site site) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return false;
		}
		boolean success = false;
		switch (site.getFamily()) {
		case Site.LOG_SITE:
			success = LogPool.getInstance().update(site);
			break;
		case Site.DATA_SITE:
			success = DataPool.getInstance().update(site);
			break;
		case Site.CALL_SITE:
			success = CallPool.getInstance().update(site);
			break;
		case Site.WORK_SITE:
			success = WorkPool.getInstance().update(site);
			break;
		case Site.BUILD_SITE:
			success = BuildPool.getInstance().update(site);
			break;
		}
		return success;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findTable(java.lang.String, java.lang.String)
	 */
	@Override
	public Table findTable(String db, String table) throws VisitException {
		return Launcher.getInstance().findTable(db, table);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#createSpace(com.lexst.sql.schema.Table)
	 */
	@Override
	public boolean createSpace(Table table) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return false;
		}
		return Launcher.getInstance().createSpace(table);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#deleteSpace(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean deleteSpace(String db, String table) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return false;
		}
		return Launcher.getInstance().deleteSpace(db, table);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#rebuild(java.lang.String, java.lang.String, short, com.lexst.util.host.IP[])
	 */
	@Override
	public Address[] rebuild(String db, String table, short columnId, Address[] hosts) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return null;
		}
		return DataPool.getInstance().rebuild(new Space(db, table), columnId, hosts);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#loadIndex(java.lang.String, java.lang.String, com.lexst.util.host.IP[])
	 */
	@Override
	public Address[] loadIndex(String db, String table, Address[] hosts) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return null;
		}
		return DataPool.getInstance().loadIndex(new Space(db, table), hosts);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#stopIndex(java.lang.String, java.lang.String, com.lexst.util.host.IP[])
	 */
	@Override
	public Address[] stopIndex(String db, String table, Address[] hosts) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return null;
		}
		return DataPool.getInstance().stopIndex(new Space(db ,table), hosts);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#loadChunk(java.lang.String, java.lang.String, com.lexst.util.host.IP[])
	 */
	@Override
	public Address[] loadChunk(String db, String table, Address[] hosts) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return null;
		}
		return DataPool.getInstance().loadChunk(new Space(db, table), hosts);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#stopChunk(java.lang.String, java.lang.String, com.lexst.util.host.IP[])
	 */
	@Override
	public Address[] stopChunk(String db, String table, Address[] hosts) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return null;
		}
		return DataPool.getInstance().stopChunk(new Space(db, table), hosts);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#buildTask(java.lang.String, com.lexst.util.host.IP[])
	 */
	@Override
	public Address[] buildTask(String naming, Address[] hosts) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return null;
		}
		return BuildPool.getInstance().buildTask(naming, hosts);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#agree(long, com.lexst.util.host.SiteHost)
	 */
	@Override
	public boolean agree(long chunkId, SiteHost local) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return false;
		}
		return BuildPool.getInstance().agree(chunkId, local);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#publish(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, long, long)
	 */
	@Override
	public boolean publish(SiteHost local, String db, String table, long chunkid, long length) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return false;
		}
		Space space = new Space(db, table);
		return DataPool.getInstance().distribute(local, space, chunkid, length);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#upgrade(com.lexst.util.host.SiteHost, java.lang.String, java.lang.String, long[], long[])
	 */
	@Override
	public boolean upgrade(SiteHost from, String db, String table,
			long[] oldIds, long[] newIds) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return false;
		}
		Space space = new Space(db, table);
		return DataPool.getInstance().upgrade(from, space, oldIds, newIds);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#accede(java.lang.String, java.lang.String, long, long, long)
	 */
	@Override
	public boolean accede(String db, String table, long chunkid, long length,
			long modified) throws VisitException {
		if (!Launcher.getInstance().isRunsite()) {
			return false;
		}
		Space space = new Space(db, table);
		return BuildPool.getInstance().accede(space, chunkid, length, modified);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findBuildChunk(java.lang.String)
	 */
	@Override
	public long[] findBuildChunk(String naming) throws VisitException {
		return BuildPool.getInstance().findBuildChunk(naming);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#showSite(int)
	 */
	@Override
	public SiteHost[] showSite(int site) throws VisitException {
		List<SiteHost> elements = null;
		ArrayList<SiteHost> array = new ArrayList<SiteHost>();
		switch (site) {
		case 0: //all site
			elements = LogPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			elements = DataPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			elements = WorkPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			elements = BuildPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			elements = CallPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			break;
		case Site.LOG_SITE:
			elements = LogPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			break;
		case Site.DATA_SITE:
			elements = DataPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			break;
		case Site.WORK_SITE:
			elements = WorkPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			break;
		case Site.BUILD_SITE:
			elements = BuildPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			break;
		case Site.CALL_SITE:
			elements = CallPool.getInstance().gather();
			if (elements != null) array.addAll(elements);
			break;
		}
		
		if (array.isEmpty()) return null;
		SiteHost[] hosts = new SiteHost[array.size()];
		return array.toArray(hosts);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#showChunkSize(java.lang.String, int, com.lexst.util.host.Address[])
	 */
	@Override
	public long showChunkSize(String schema, int siteType, Address[] sites) throws VisitException {
		return DataPool.getInstance().showChunkSize(schema, siteType, sites);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#showChunkSize(java.lang.String, java.lang.String, int, com.lexst.util.host.Address[])
	 */
	@Override
	public long showChunkSize(String schema, String table, int siteType, Address[] sites) throws VisitException {
		return DataPool.getInstance().showChunkSize(schema, table, siteType, sites);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#showTask(java.lang.String, java.lang.String[])
	 */
	@Override
	public String showTask(String tag, Address[] sites) throws VisitException {
		return Launcher.getInstance().showTask(tag, sites);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#sendCodePoints(java.lang.String, byte[])
	 */
	@Override
	public boolean sendCodePoints(String compress, byte[] stream) throws VisitException {
		return CodePointPool.getInstance().save(compress, stream);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.visit.naming.home.HomeVisit#findCodePoints(java.lang.String, java.lang.String, short)
	 */
	@Override
	public int[] findCodePoints(String schema, String table, short columnId) throws VisitException {
		return CodePointPool.getInstance().find(new Docket(schema, table, columnId));
	}
}