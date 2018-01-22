/**
 * 
 */
package com.lexst.live.pool;

import java.io.*;
import java.util.*;

import com.lexst.log.client.*;
import com.lexst.remote.client.call.*;
import com.lexst.remote.client.top.*;
import com.lexst.site.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;


public class SQLCaller  {

	private static int insertIndex = 0;
	
	/**
	 * 
	 */
	public SQLCaller() {
		super();
	}
	
	/**
	 * apply a top client handle
	 * @return
	 * @throws VisitException
	 */
	private TopClient solicit(SocketHost remote) {
		TopClient client = new TopClient(true);
		try {
			client.connect(remote);
			return client;
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		return null;
	}

	/**
	 * apply a top client handle
	 * @return
	 * @throws VisitException
	 */
	private CallClient fetch(SocketHost remote) {
		CallClient client = new CallClient(true);
		try {
			client.connect(remote);
			return client;
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		return null;
	}

	/**
	 * release connect and close socket
	 * @param client
	 */
	private void complete(CallClient client) {
		if (client == null) return;
		try {
			client.exit();
			client.close();
		} catch (IOException exp) {
			Logger.error(exp);
		}
	}
	
	/**
	 * release connect and close socket
	 * @param client
	 */
	private void complete(TopClient client) {
		if (client == null) return;
		try {
			client.exit();
			client.close();
		} catch (IOException exp) {

		}
	}
	
	private SiteHost[] selectCallSite(SiteHost top, Site local, Space space) {
		TopClient client = solicit(top.getStreamHost());
		if (client == null) return null;

		SiteHost[] hosts = null;
		try {
			hosts = client.selectCallSite(space.getSchema(), space.getTable());
		} catch (VisitException exp) {
			Logger.error(exp);
		}
		this.complete(client);

		return hosts;
	}
	
	private SiteHost[] selectCallSite(SiteHost top, Site local, String naming) {
		TopClient client = solicit(top.getStreamHost());
		if(client == null) return null;
		
		SiteHost[] hosts = null;
		try {
			hosts = client.selectCallSite(naming);
		} catch (VisitException exp) {
			Logger.error(exp);
		}
		this.complete(client);
		return hosts;
	}
	
//	private SiteHost[] selectCallSite(SiteHost top, Site local, String naming, Space space) {
//		TopClient client = solicit(top.getTCPHost());
//		if(client == null) return null;
//		
//		SiteHost[] hosts = null;
//		try {
//			hosts = client.selectCallSite(naming, space);
//		} catch (VisitException exp) {
//			Logger.error(exp);
//		}
//		this.complete(client);
//		return hosts;
//	}
	
	/**
	 * 查找保有某一组的SPACE的call节点服务器地址
	 * @param top
	 * @param local
	 * @param naming
	 * @param spaces
	 * @return
	 */
	private SiteHost[] selectCallSite(SiteHost top, Site local, String naming, List<Space> spaces) {
		TreeSet<SiteHost> set = new TreeSet<SiteHost>();

		TopClient client = solicit(top.getStreamHost());
		if (client == null) return null;

		int index = 0;
		try {
			for (; index < spaces.size(); index++) {
				SiteHost[] hosts = client.selectCallSite(naming, spaces.get(index));
				SiteSet sites = new SiteSet(hosts);
				if (index == 0) {
					// 第一次保存全部
					set.addAll(sites.list());
				} else {
					// 执行"AND"操作(交)
					set.retainAll(sites.list());
				}
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		}
		this.complete(client);
		// 出错
		if(index < spaces.size()) return null;
		
		int size = set.size();
		if (size == 0) return null;
		SiteHost[] hosts = new SiteHost[size];
		return set.toArray(hosts);
	}
	
	/**
	 * @param hosts
	 * @param select
	 * @param dc
	 * @param findWorkIP
	 * @return
	 */
	public byte[] select(SiteHost top, Site local, Select select) {
		Space space = select.getSpace();
		SiteHost[] hosts = selectCallSite(top, local, space);
		if (hosts == null || hosts.length == 0) {
			Logger.error("SQLCaller.select, cannot find call site");
			return null;
		}
		
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024 * 512);
		
		for(int i = 0; i < hosts.length; i++) {
			CallClient client = this.fetch(hosts[i].getStreamHost());
			if(client == null) {
				Logger.error("SQLCaller.select, cannot connect call site %s", hosts[i]);
				continue;
			}
			try {
				byte[] data = client.select(select);
				if(data != null  && data.length>0) {
					buff.write(data, 0, data.length);
				}
			} catch (VisitException exp) {
				Logger.error(exp);
			}
			this.complete(client);
		}
		if(buff.size() == 0) return null;
		return buff.toByteArray();
	}
	
	/**
	 * 执行分布计算
	 * @param top
	 * @param local
	 * @param conduct
	 * @return
	 */
	public byte[] conduct(SiteHost top, Site local, Conduct conduct) {		
		// 查找匹配的CALL节点服务器地址
		FromInputObject object = conduct.getFrom().getInput();
		String naming = object.getNamingString();
		List<Space> spaces = object.getSelectSpaces();
		
		// 查找对应的CALL节点
		SiteHost[] hosts = null;
		if (spaces.size() > 0) {
			hosts = selectCallSite(top, local, naming, spaces);
		} else {
			hosts = selectCallSite(top, local, naming);
		}
		// 没找到CALL节点地址
		if (hosts == null || hosts.length == 0) {
			Logger.error("SQLCaller.adc, cannot find call site");
			return null;
		}

		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024 * 1024);
		
		// 从多个CALL服务器上取数据(CALL地址从属于不同的HOME集群下)
		for(int i = 0; i < hosts.length; i++) {
			CallClient client = fetch(hosts[i].getStreamHost());
			if(client == null) {
				Logger.error("SQLCaller.adc, cannot connect call site %s", hosts[i]);
				continue;
			}
			try {
				byte[] data = client.conduct(conduct);
				if (data != null && data.length > 0) {
					buff.write(data, 0, data.length);
				}
			} catch (VisitException exp) {
				Logger.error(exp);
			}
			this.complete(client);
		}
		if(buff.size() == 0) return null;
		return buff.toByteArray();
	}


		
	public long delete(SiteHost top, Site local, Delete delete) {
		Space space = delete.getSpace();
		SiteHost[] hosts = selectCallSite(top, local, space);
		if (hosts == null || hosts.length == 0) {
			Logger.error("SQLCaller.delete, cannot find call site");
			return -1;
		}
		
		long deleteItems = 0;
		for(int i = 0; i < hosts.length; i++) {
			CallClient client = this.fetch(hosts[i].getStreamHost());
			if(client == null) {
				Logger.error("SQLCaller.delete, cannot connect call site %s", hosts[i]);
				continue;
			}
			
			try {
				long num = client.delete(delete);
				if (num > 0) deleteItems += num;
			} catch (VisitException exp) {
				Logger.error(exp);
			}
			this.complete(client);
		}
		return deleteItems;
	}
		
	private static synchronized int index(int num) {
		if( insertIndex >= num) insertIndex = 0;
		return insertIndex = 0;
	}
	
	/**
	 * 插入数据记录
	 * @param top
	 * @param local
	 * @param insert
	 * @return
	 */
	public int insert(SiteHost top, Site local, Insert insert) {
		Space space = insert.getSpace();
		SiteHost[] hosts = selectCallSite(top, local, space);
		if (hosts == null || hosts.length == 0) {
			Logger.error("SQLCaller.insert, cannot find call site");
			return -1;
		}

		int index = SQLCaller.index(hosts.length);

		CallClient client = this.fetch(hosts[index].getStreamHost());
		if(client == null) {
			Logger.error("SQLCaller.insert, cannot connect call site %s", hosts[index]);
			return -1;
		}

		int stamp = -1;
		try {
			stamp = client.insert(insert, true);
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable e) {
			Logger.fatal(e);
		}
		this.complete(client);
		
		Logger.debug("SQLCaller.insert, return stamp:%d", stamp);

		return stamp;
	}
	
	/**
	 * 写入一组记录
	 * @param top
	 * @param local
	 * @param inject
	 * @return
	 */
	public int inject(SiteHost top, Site local, Inject inject) {
		Space space = inject.getSpace();
		SiteHost[] hosts = selectCallSite(top, local, space);
		if (hosts == null || hosts.length == 0) {
			Logger.error("SQLCaller.inject, cannot find call site");
			return -1;
		}

		int index = SQLCaller.index(hosts.length);

		CallClient client = this.fetch(hosts[index].getStreamHost());
		if(client == null) {
			Logger.error("SQLCaller.inject, cannot connect call site %s", hosts[index]);
			return -1;
		}

		int stamp = -1;
		try {
			stamp = client.inject(inject, true);
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable e) {
			Logger.fatal(e);
		}
		this.complete(client);
		
		Logger.debug("SQLCaller.inject, return stamp:%d", stamp);

		return stamp;
	}
	
	public long update(SiteHost top, Site local, Update update) {
		Space space = update.getSpace();
		SiteHost[] hosts = selectCallSite(top, local, space);
		if (hosts == null || hosts.length == 0) {
			Logger.error("SQLCaller.update, cannot find call site");
			return -1;
		}
		
		long count = 0;
		for(int i = 0; i < hosts.length; i++) {
			CallClient client = this.fetch(hosts[i].getStreamHost());
			if(client == null) {
				Logger.error("SQLCaller.update, cannot connect call site %s", hosts[i]);
				continue;
			}
			
			try {
				long num = client.update(update);
				if (num > 0) count += num;
			} catch (VisitException exp) {
				Logger.error(exp);
			}
			this.complete(client);	
		}
		return count;
	}

}