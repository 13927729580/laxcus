/**
 * 
 */
package com.lexst.call.pool;

import java.io.*;
import java.util.*;

import com.lexst.algorithm.balance.*;
import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.remote.client.work.*;
import com.lexst.site.call.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;
import com.lexst.sql.statement.*;
import com.lexst.util.host.*;

/**
 * 
 * 为"aggregate"计算提供服务
 *
 */
public class WorkPool extends JobPool {
	
	private static WorkPool selfHandle = new WorkPool();
	
//	/** 任务命名 -> WORK节点集合  **/	
//	private Map<Naming, SiteSet> mapNaming = new TreeMap<Naming, SiteSet>();
//	
//	/* site address -> connect set */
//	private Map<SiteHost, ClientSet> mapClient = new TreeMap<SiteHost, ClientSet>();
//	
//	/** HOME节点通知，更新全部WORK节点记录  **/
//	private boolean refresh;

	/**
	 * default
	 */
	private WorkPool() {
		super();
	}
	
	/**
	 * @return
	 */
	public static WorkPool getInstance() {
		return WorkPool.selfHandle;
	}
	
	/**
	 * 解析分布数据图谱
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	private List<DiskArea> resolveArea(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		// 解析返回结果头
		ReturnTag tag = new ReturnTag();
		int size = tag.resolve(b, seek, end - seek);
		seek += size;

		// 跨过头尺寸，解析 Area
		ArrayList<DiskArea> array = new ArrayList<DiskArea>();
		while(seek < end) {
			DiskArea area = new DiskArea();
			size = area.resolve(b, seek, end - seek);
			seek += size;
			array.add(area);
		}
		return array;
	}

	/**
	 * 根据WORK节点地址，建立一个连接句柄
	 * @param host
	 * @param stream
	 * @return
	 */
	private WorkClient createClient(SiteHost host, boolean stream) {
		boolean success = false;
		// 连接服务器
		SocketHost address = (stream ? host.getStreamHost() : host.getPacketHost());
		WorkClient client = new WorkClient(address);
		try {
			client.reconnect();
			success = true;
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		if (!success) {
			client.close();
			return null;
		}
		
		return client;
	}
	
	/**
	 * 以数据流模式，建立连接
	 * @param host
	 * @return
	 */
	private WorkClient createClient(SiteHost host) {
		return createClient(host, true);
	}
	
	/**
	 * 启动conduct计算
	 * 
	 * @param task
	 * @param object
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	private byte[] conduct(BalanceTask task, ToOutputObject object, byte[] b, int off, int len)  {
		String naming = object.getNamingString();
		int sites = object.phases();
		List<DiskArea> areas = this.resolveArea(b, off, len);

		NetDomain[] domains = null;
		// 在有平衡任务接口时，由用户定义的平均算法处理分布数据
		if (task != null) {
			// 根据本阶段命名和WORK节点数,由平衡处理任务进行分片
			try {
				domains = task.split(naming, sites, areas);
			} catch (BalanceTaskException e) {
				Logger.error(e);
				return null;
			} catch (Throwable e) {
				Logger.fatal(e);
				return null;
			}
		}
		// 没有用户定义的资源平衡算法,采用默认计算
		if (domains == null) {
			NetMatrix module = new NetMatrix(areas);
			domains = module.balance(sites);
		}
		
		// 数据片段的数量绝不可以超过预定连接数量
		if (domains.length > sites) {
			Logger.error("WorkPool.conduct, section size out!");
			return null;
		}
		// 以数据片段数量和预定连接量不一致时，以数据片段数为准(连接量可能会缩小，但是不会增大)
		if (domains.length != sites) {
			sites = domains.length;
		}
		
		// 启动委托代理器， 预定客户端连接数
		WorkTrustor trustor = new WorkTrustor();
		trustor.setJobs(sites);
		// 线程工作完成后立即退出(不保持线程状态)
		trustor.setKeepThread(false);
		
		SiteSet success = new SiteSet( sites );
		ArrayList<Integer> failed = new ArrayList<Integer>(sites);
		
		for (int index = 0; index < sites; index++) {
			ToPhase phase = object.getPhase(index);
			SiteHost remote = phase.getRemote();
			// 连接WORK节点
			WorkClient client = this.createClient(remote);
			if (client == null) {
				Logger.error("WorkPool.conduct, cannot find site %s", remote);
				// 记录失败的队列下标
				failed.add(index);
				continue;
			}
			
			// 生成数据片段
//			byte[] data = domains[index].build();	
			// 立即启动conduct操作(区别全部存储/并发执行的push模式，这是串行模式)
			trustor.send(client, phase, domains[index]);
			// 发送成功，保存这个节点地址
			success.add(remote);
		}
		
		// 一个连接成功都没有时，这种情况可能是网络错误故障(大批量计算情况下)
		if(success.isEmpty()) {
			Logger.error("WorkPool.conduct, error! exit!");
			return null;
		}
		
		// 如果有失败，使用已经连接成功的主机进行重连
		if (failed.size() > 0) {
			int[] groups = new int[failed.size()];
			for (int i = 0; i < groups.length; i++) {
				groups[i] = failed.get(i).intValue();
			}
			for (int i = 0; i < groups.length; i++) {
				int index = groups[i];
				SiteHost remote = success.next();
				ToPhase phase = object.getPhase(index);
				WorkClient client = this.createClient(remote);
				// 仍然出错，退出!
				if (client != null) {
					Logger.error("WorkPool.conduct, retry error! exit!");
					break;
				}
				// 生成分片数据和发送
//				byte[] data = domains[index].build();
				trustor.send(client, phase, domains[index]);
			}
		}
		
		// 等于任务完成，aggregate阶段完成
		trustor.waiting();
		// 取数据
		byte[] data = trustor.data();
		// 返回结果
		return data;
	}
	
	/**
	 * 启动conduct计算
	 * @param conduct
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public byte[] conduct(Conduct conduct, byte[] b, int off, int len) {
		// 取数据平衡算法
		BalanceTask task = null;
		BalanceObject balance = conduct.getBalance();
		if (balance != null) {
			task = BalanceTaskPool.getInstance().find(balance.getNaming());
		}

		// 从根输出对象开始，迭代计算。上一级的输出是下一级的输入。
		ToOutputObject object = conduct.getTo().getOutput();
		byte[] data = this.conduct(task, object, b, off, len);
		object = object.next();

		while (object != null) {
			data = this.conduct(task, object, data, 0, data.length);
			object = object.next();
		}
		// 输出结果
		return data;
	}
	
//	public void refresh() {
//		this.refresh = true;
//		this.wakeup();
//	}
//
//	/**
//	 * 根据命名查找对应的WORK站点地址集合
//	 * @param naming
//	 * @return
//	 */
//	private SiteSet findWorkNodes(Naming naming) {
//		super.lockMulti();
//		try {
//			return mapNaming.get(naming);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//		return null;
//	}
//	
//	/**
//	 * find a data client 
//	 * @param host
//	 * @return
//	 */
//	private WorkClient findClient(SiteHost host, boolean stream) {
//		ClientSet set = null;
//		super.lockMulti();
//		try {
//			set = mapClient.get(host);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockMulti();
//		}
//
//		if (set != null && set.size() > 0) {
//			WorkClient client = (WorkClient) set.lockNext();
//			if (client != null) return client;
//			if (set.size() >= ClientSet.LIMIT) {
//				client = (WorkClient) set.next();
//				client.locking();
//				return client;
//			}
//		}
//		
//		boolean success = false;
//		// connect to host
//		SocketHost address = (stream ? host.getTCPHost() : host.getUDPHost());
//		WorkClient client = new WorkClient(address);
//		try {
//			client.reconnect();
//			success = true;
//		} catch (IOException exp) {
//			Logger.error(exp);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		}
//		if (!success) {
//			client.close();
//			return null;
//		}
//
////		client.locking();	// locked client
//		client.start(); 	// start client thread
//
//		super.lockSingle();
//		try {
//			if(set == null) {
//				set = new ClientSet();
//				mapClient.put(host, set);
//			}
//			set.add(client);
//		} catch(Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//		
//		return client;
//	}
//	
//	private WorkClient findClient(SiteHost host) {
//		return findClient(host, true);
//	}
//	
//
//	
////	private byte[] subConduct(ToOutputObject object, byte[] b, int off, int len){
////		List<Area> array = this.resolveArea(b, off, len);
////	
////		
////	}
//
//	
////	/**
////	 * 执行子检索
////	 * 
////	 * @param conduct
////	 * @param sub
////	 * @param b
////	 * @param off
////	 * @param len
////	 * @return
////	 */
////	private byte[] subConduct(Conduct conduct, ToObject sub, byte[] b, int off, int len){
////		List<Area> array = this.resolveArea(b, off, len);
////		
////		Naming naming = sub.getTaskNaming();
////		SiteSet set = findWorkNodes(naming);
////		if(set == null || set.isEmpty()) {
////			Logger.error("WorkPool.subConduct, cannot find naming '%s'", naming);
////			return null;
////		}
////
////		// 未定义节点数,或者要求节点数超过实际节点数时,以实际节点数为准
////		int sites = sub.getSites();
////		if (sites < 1 || sites > set.size()) {
////			sites = set.size();
////		}
////
////		// 启动标准平衡计算
////		Matrix module = new Matrix(array);
////		Domain[] tables = module.balance(sites);
////
////		// 启动WORK委托服务器，监管WORK运行
////		WorkTrustor trustor = new WorkTrustor(tables.length);
////
////		for (int index = 0; index < tables.length; index++) {
////			SiteHost host = set.next();
////			WorkClient client = this.findClient(host);
////			if (client == null) {
////				Logger.error("WorkPool.subConduct, cannot find site %s", host);
////				break;
////			}
////			byte[] data = tables[index].build();
////			Conduct clone_conduct = (Conduct) conduct.clone();
////			trustor.add(client, clone_conduct, data);
////		}
////		
////		if(trustor.size() != tables.length) {
////			Logger.error("WorkPool.subConduct, client not match!");
////			trustor.disconnect(true);
////			return null;
////		}
////
////		// 启动任务
////		trustor.launch();
////		// 等于"aggregate"任务
////		trustor.waiting();
////		// 取数据
////		byte[] data = trustor.data();
////		// 返回数据
////		return data;
////	}
//	
////	/**
////	 * @param conduct
////	 * @param files
////	 * @return
////	 */
////	public byte[] conduct(Conduct conduct, byte[] b, int off, int len) {
////		List<Area> files = this.resolveArea(b, off, len);
////		
////		ToObject tobj = conduct.getTo();
////		Naming naming = tobj.getTaskNaming();
////		Logger.debug("WorkPool.conduct, aggregate naming:%s", naming);
////
////		// 如果已经指定SITE地址，下面就不需要指定了
////		
////		// 在地址指定的情况下，指定地址
////		SiteSet set = findWorkNodes(naming);
////		if(set == null || set.isEmpty()) {
////			Logger.error("WorkPool.conduct, cannot find naming '%s'", naming);
////			return null;
////		}
////
////		// 未定义节点数,或者要求节点数超过实际节点数时,以实际节点数为准
////		int sites = conduct.getTo().getSites();
////		if (sites < 1 || sites > set.size()) {
////			sites = set.size();
////		}
////				
////		// 在有平衡任务接口时，由用户定义的平均算法处理分布数据
////		Domain[] tables = null;
////		BalanceObject balance = conduct.getBalance();
////		if (balance != null) {
////			BalanceTask task = BalanceTaskPool.getInstance().find(balance.getNaming());
////			if (task != null) {
////				tables = task.split(conduct, files);
////			}
////		}
////		
////		// 没有用户定义的资源平衡算法,采用默认计算
////		if(tables == null) {
////			Matrix module = new Matrix(files);
////			tables = module.balance(sites);
////		}
////		
////		// 启动WORK委托服务器，监管WORK运行
////		WorkTrustor trustor = new WorkTrustor(tables.length);
////
////		for (int index = 0; index < tables.length; index++) {
////			SiteHost host = set.next();
////			WorkClient client = this.findClient(host);
////			if (client == null) {
////				Logger.error("WorkPool.conduct, cannot find site %s", host);
////				break;
////			}
////			byte[] data = tables[index].build();
////			Conduct clone_conduct = (Conduct) conduct.clone();
////			trustor.add(client, clone_conduct, data);
////		}
////		
////		if(trustor.size() != tables.length) {
////			Logger.error("WorkPool.conduct, client not match!");
////			trustor.disconnect(true);
////			return null;
////		}
////
////		// 启动任务
////		trustor.launch();
////		// 等于"aggregate"任务
////		trustor.waiting();
////		// 取数据
////		byte[] data = trustor.data();
////		
////		// 判断如果有子级链，继续!
////		ToObject sub = tobj.next();
////		while(sub != null) {
////			if(data == null) {
////				Logger.error("WordkPool.conduct, error! is null!");
////				break;
////			}
////			data = subConduct(conduct, sub, data, 0, data.length);
////			sub = tobj.next();
////		}
////		
////		Logger.debug("WorkPool.conduct, result data size:%d", (data == null ? -1 : data.length));
////		
////		return data;
////	}
//	
//
//	
////	public byte[] conduct2(Conduct conduct,  byte[] b, int off, int len) {
////		List<Area> files = this.resolveArea(b, off, len);
////		ToOutputObject root = conduct.getTo().getOutput();
////		int sites = root.size();
////		
////		// 在有平衡任务接口时，由用户定义的平均算法处理分布数据
////		Domain[] tables = null;
////		BalanceObject balance = conduct.getBalance();
////		if (balance != null) {
////			BalanceTask task = BalanceTaskPool.getInstance().find(balance.getNaming());
////			if (task != null) {
////				tables = task.split(conduct, files);
////			}
////		}
////		// 没有用户定义的资源平衡算法,采用默认计算
////		if (tables == null) {
////			Matrix module = new Matrix(files);
////			tables = module.balance(sites);
////		}
////
////		WorkTrustor trustor = new WorkTrustor(sites);
////		
////		for (int index = 0; index < sites; index++) {
////			ToPhase phase = root.get(index);
////			SiteHost host = phase.getRemote();
////			WorkClient client = this.findClient(host);
////			if (client == null) {
////				Logger.error("WorkPool.conduct, cannot find site %s", host);
////				break;
////			}
////			byte[] data = tables[index].build();
////			trustor.add(client, phase, data);
////		}
////		
////		// 启动任务
////		trustor.launch();
////		// 等于"aggregate"任务
////		trustor.waiting();
////		// 取数据
////		byte[] data = trustor.data();
////		
////		// 判断如果有子级链，继续!
////		ToOutputObject sub = root.next();
////		while(sub != null) {
////			if(data == null) {
////				Logger.error("WordkPool.conduct, error! is null!");
////				break;
////			}
////			data = subConduct(conduct, sub, data, 0, data.length);
////			sub = sub.next();
////		}
////		
////		return null;
////	}
//	
//	/**
//	 * 根据AGGREGATE命名，找到对应的WORK节点地址集合
//	 * @param naming
//	 * @return
//	 */
//	private List<SiteHost> find(String naming) {
//		return find(new Naming(naming));
//	}
//
//	/**
//	 * 根据AGGREGATE命名，找到对应的WORK节点地址集合
//	 * @param naming
//	 * @return
//	 */
//	private List<SiteHost> find(Naming naming) {
//		List<SiteHost> array = new ArrayList<SiteHost>();
//
//		// 检查内存中的记录
//		SiteSet set = findWorkNodes(naming);
//		if(set != null && set.size() > 0) {
//			array.addAll(set.list());
//			return array;
//		}
//		
//		// 连接HOME节点，取得对应的WORK节点地址
//		HomeClient client = super.bring(false);
//		if(client == null) return null;
//		try {
//			SiteHost[] hosts = client.findWorkSite(naming.get());
//			// 记录新WORK节点集合
//			if (hosts != null && hosts.length > 0) {
//				set = new SiteSet(hosts);
//				array.addAll(set.list());
//			}
//		} catch (VisitException exp) {
//			Logger.error(exp);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		}
//		this.complete(client);
//		
//		// 没有，退出
//		if (array.isEmpty()) return null;
//
//		// 保存新的WORK地址集合
//		super.lockSingle();
//		try {
//			mapNaming.put(naming, set);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//		return array;
//	}
//	
//	/**
//	 * 
//	 */
//	private void refreshSite() {
//		HomeClient client = super.bring(false);
//		if (client == null) {
//			Logger.error("WorkPool.refreshSite, cannot connect home-site:%s", home);
//			return;
//		}
//
//		boolean error = true;
//		WorkSite[] sites = null;
//		try {
//			sites = (WorkSite[]) client.batchWorkSite();
//			error = false;
//		} catch (VisitException exp) {
//			Logger.error(exp);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		}
//		this.complete(client);
//		
//		if (error) {
//			Logger.warning("WorkPool.refreshSite, visit error!");
//			return;
//		}
//		
//		Logger.debug("WorkPool.refreshSite, work site size:%d", (sites == null ? -1 : sites.length));
//
//		List<SiteHost> allsite = new ArrayList<SiteHost>();
//
//		super.lockSingle();
//		try {
//			mapNaming.clear();
//			for (int i = 0; sites != null && i < sites.length; i++) {
//				SiteHost host = sites[i].getHost();
//				allsite.add(host);
//				for (Naming naming : sites[i].list()) {
//					SiteSet set = mapNaming.get(naming);
//					if (set == null) {
//						set = new SiteSet();
//						mapNaming.put(naming, set);
//					}
//					set.add(host);
//				}
//			}
//			
//			// release exclude client
//			List<SiteHost> excludes = new ArrayList<SiteHost>();
//			for (SiteHost host : mapClient.keySet()) {
//				if (!allsite.contains(host)) excludes.add(host);
//			}
//			for (SiteHost host : excludes) {
//				ClientSet set = mapClient.get(host);
//				int size = set.size();
//				for (int i = 0; i < size; i++) {
//					WorkClient ws = (WorkClient) set.get(i);
//					ws.stop();
//				}
//			}
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//
////		CallSite site = super.callInstance.getLocal();
////		// update all naming
////		site.clearAggregateNaming();
////		for(Naming naming: mapNaming.keySet()) {
////			Logger.debug("WorkPool.refreshSite, add naming:%s", naming);
////			site.addAggregateNaming(naming.toString());
////		}
//		
//		CallSite site = super.callInstance.getLocal();
//		site.updateToNamings( mapNaming.keySet());
//		// relogin
//		super.callInstance.setOperate(BasicLauncher.RELOGIN);
//	}
//	
//	private void check() {
//		if (refresh) {
//			this.refresh = false;
//			this.refreshSite();
//		}
//	}
//	
//	/**
//	 * stop all client
//	 */
//	private void stopClients() {
//		if(mapClient.isEmpty()) return;
//		
//		super.lockSingle();
//		try {
//			ArrayList<SiteHost> array = new ArrayList<SiteHost>(mapClient.keySet());
//			for (SiteHost host : array) {
//				ClientSet set = mapClient.remove(host);
//				int size = set.size();
//				for (int i = 0; i < size; i++) {
//					WorkClient client = (WorkClient) set.get(i);
//					client.stop();
//				}
//			}
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			super.unlockSingle();
//		}
//	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("WorkPool.process, into...");

		this.delay(10000);
//		refresh = true;
		
		while (!super.isInterrupted()) {
//			this.check();
			this.delay(5000);
		}
		Logger.info("WorkPool.process, exit");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
//		this.stopClients();
	}

}