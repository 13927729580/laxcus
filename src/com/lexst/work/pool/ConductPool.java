/**
 * @email admin@wigres.com
 *
 */
package com.lexst.work.pool;

import java.io.*;

import com.lexst.algorithm.disk.*;
import com.lexst.algorithm.to.*;
import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.remote.*;
import com.lexst.remote.client.data.*;
import com.lexst.remote.client.work.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;
import com.lexst.util.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;
import com.lexst.visit.*;

/**
 * 分布计算的"aggregate"阶段实现类
 *
 */
public class ConductPool extends JobPool {
	
	private static ConductPool selfHandle = new ConductPool();

	/**
	 * 
	 */
	private ConductPool() {
		super();
	}
	
	/**
	 * @return
	 */
	public static ConductPool getInstance() {
		return ConductPool.selfHandle;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// TODO Auto-generated method stub
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		while(!super.isInterrupted()) {
			this.delay(10000);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		Logger.info("ConductPool.finish, exit!");
	}
	
	/**
	 * 解析数据流，返回ToPhase和Domain两个对象
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private Object[] split(byte[] b, int off, int len) throws IOException, ClassNotFoundException {
		int end = off + len;
		int seek = off;

		// 解析ToPhase
		if (seek + 4 > end) {
			throw new SizeOutOfBoundsException("conduct size missing!");
		}
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (seek + size > end) {
			throw new SizeOutOfBoundsException("conduct size missing!");
		}
		Apply apply1 = Apply.resolve(b, seek, size);
		ToPhase phase = (ToPhase) apply1.getParameters()[0];
		seek += size;

		// 解析domain
		if (seek + 4 > end) {
			throw new SizeOutOfBoundsException("conduct size missing!");
		}
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (seek + size > end) {
			throw new SizeOutOfBoundsException("conduct size missing!");
		}
		Apply apply2 = Apply.resolve(b, seek, size);
		NetDomain domain = (NetDomain) apply2.getParameters()[0];
		seek += size;
		
		return new Object[] { phase, domain };
	}

	/**
	 * 执行"aggregate"阶段计算
	 * 
	 * @param phase
	 * @param domain
	 * @param stream
	 * @return
	 */
	private Entity conduct(ToPhase phase, NetDomain domain, boolean stream) {
		//1. 根据命名，找到对应的实例
		Naming naming = phase.getNaming();
		ToTask task = ToTaskPool.getInstance().find(naming);
		if(task == null) {
			Logger.error("Launcher.conduct, cannot find naming:%s", naming);
			Command cmd = new Command(Response.CONDUCT_SERVERERR);
			return (stream ? new Stream(cmd) : new Packet(cmd));
		}
		task.setToPhase(phase);

		//2. 第1次连接DATA节点，此后连接WORK节点(WORK迭代)
		if (phase.isSlaveLink()) {
			this.suckFromWorkSites(task, domain);
		} else {
			this.suckFromDataSites(task, domain);
		}
		
		Logger.debug("Launcher.conduct, into execute...");

		//3. 合并来自不同节点上的数据，完成最后数据计算。返回结果有两种:Area字节流或者实际结果数据流
		byte[] data = null;
		try {
			data = task.complete(DiskPool.getInstance());
		} catch (ToTaskException e) {
			Logger.error(e);
		} catch (Throwable e) {
			Logger.fatal(e);
		}
		
		Logger.debug("Launcher.conduct, execute finished! result size:%d", (data == null ? -1 : data.length));
		
		// 生成流或者包实体，保存结果数据，发送给调用节点
		Command cmd = new Command(Response.CONDUCT_OKAY);
		Entity reply = (stream ? new Stream(cmd) : new Packet(cmd));
		reply.setData(data);
		return reply;
	}
	
	/**
	 * 解析数据流，执行分布计算
	 * @param b
	 * @param off
	 * @param len
	 * @param stream
	 * @return
	 */
	public Entity conduct(byte[] b, int off, int len, boolean stream) {
		// 解析数据流
		Object[] objects = null;
		try {
			objects = split(b, off, len);
		} catch (IOException e) {
			Logger.error(e);
		} catch (ClassNotFoundException e) {
			Logger.error(e);
		} catch (SizeOutOfBoundsException e) {
			Logger.error(e);
		} catch (Throwable t) {
			Logger.fatal(t);
		}
		// 出错
		if (objects == null) {
			Command cmd = new Command(Response.CONDUCT_CLIENTERR);
			return (stream ? new Stream(cmd) : new Packet(cmd));
		}

		// 执行分布计算
		ToPhase phase = (ToPhase) objects[0];
		NetDomain domain = (NetDomain) objects[1];
		return this.conduct(phase, domain, stream);
	}

	/**
	 * 连接DATA节点，提取实际数据
	 * @param task
	 * @param domain
	 */
	private void suckFromDataSites(ToTask task, NetDomain domain) {
		//启动data client抓取数据
		for (SiteHost host : domain.keySet()) {
			DiskArea area = domain.get(host);
			long jobid = area.getJobid();

			try {
				DataClient client = new DataClient(true, host.getStreamHost());
				client.reconnect();
				for (DiskField field : area.list()) {
					byte[] b = client.suckup(jobid, field);
					Logger.debug("Launcher.conduct, receive data size:%d", (b == null ? -1 : b.length));
					// 保存数据
					if (b != null && b.length > 0) {
						task.inject(field, b, 0, b.length);
					}
				}
				client.exit();
				client.close();
			} catch (ToTaskException e) {
				Logger.error(e);
			} catch (VisitException e) {
				Logger.error(e);
			} catch (IOException e) {
				Logger.error(e);
			}
		}
	}
	
	/**
	 * 连接WORK节点，根据分片信息提取实际数据
	 * @param task
	 * @param domain
	 */
	private void suckFromWorkSites(ToTask task, NetDomain domain) {
		for (SiteHost host : domain.keySet()) {
			DiskArea area = domain.get(host);
			long jobid = area.getJobid();

			try {
				WorkClient client = new WorkClient(host.getStreamHost());
				client.reconnect();
				for (DiskField field : area.list()) {
					byte[] b = client.suckup(jobid, field);
					Logger.debug("Launcher.conduct, receive data size:%d",
							(b == null ? -1 : b.length));
					// 保存数据
					if (b != null && b.length > 0) {
						task.inject(field, b, 0, b.length);
					}
				}
				client.exit();
				client.close();
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (IOException exp) {
				Logger.error(exp);
			}
		}
	}
	
}
