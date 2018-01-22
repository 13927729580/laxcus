/**
 * 
 */
package com.lexst.remote.client.work;

import java.io.*;
import java.lang.reflect.*;

import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.remote.*;
import com.lexst.remote.client.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.visit.naming.work.*;
import com.lexst.util.*;

public class WorkClient extends ThreadClient implements WorkVisit {

	private static Method methodNothing;
	
	private static Method methodSuckup;
	
	static {
		try {
			methodNothing = (WorkVisit.class).getMethod("nothing", new Class<?>[0]);
		
			methodSuckup = (WorkVisit.class).getMethod("suckup", new Class<?>[] { Long.TYPE, Integer.TYPE, Long.TYPE, Long.TYPE });
		} catch (NoSuchMethodException exp) {
			throw new NoSuchMethodError("stub class initialization failed");
		}
	}
	

	private LockArray<WorkCommand> array = new LockArray<WorkCommand>();

	/**
	 * 构造一个连接句柄，参数包括是否采用"流"模式连接和接口名
	 * 
	 * @param stream
	 * @param interfaceName
	 */
	public WorkClient(boolean stream, String interfaceName) {
		super(stream, interfaceName);
		this.setNumber(-1); // 默认不定义编号
	}

	/**
	 * 构造一个连接句柄，是否采用“流”模式连接(即TCP连接)
	 */
	public WorkClient(boolean stream) {
		this(stream, WorkVisit.class.getName());
	}

	/**
	 * @param remote
	 */
	public WorkClient(SocketHost remote) {
		this(remote.getFamily() == SocketHost.TCP);
		this.setRemote(remote);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.Visit#nothing()
	 */
	@Override
	public void nothing() throws VisitException {
		super.invoke(WorkClient.methodNothing, null);
		this.refreshTime();
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.work.WorkVisit#suckup(long, int, long, long)
	 */
	@Override
	public byte[] suckup(long jobid, int mod, long begin, long end) throws VisitException {
		this.refreshTime();
		Object[] params = new Object[] { new Long(jobid), new Integer(mod), new Long(begin), new Long(end) };
		Object param = super.invoke(WorkClient.methodSuckup, params);
		return (byte[])param;
	}

	/**
	 * 向其它WORK节点请求分片数据
	 * 
	 * @param jobid
	 * @param field
	 * @return
	 * @throws VisitException
	 */
	public byte[] suckup(long jobid, com.lexst.sql.conduct.matrix.DiskField field) throws VisitException {
		return this.suckup(jobid, field.getMod(), field.getBegin(), field.getEnd());
	}

//	/**
//	 * 执行conduct分布计算(先保存数据，再由线程完成)
//	 * @param trustor
//	 * @param phase
//	 * @param data
//	 */
//	public void conduct(WorkTrustor trustor, ToPhase phase,  byte[] data) {
//		array.add(new WorkCommand(trustor, phase, data));
//	}

	/**
	 * 执行conduct分布计算(先保存数据，再由线程完成)
	 * @param trustor
	 * @param phase
	 * @param data
	 */
	public void conduct(WorkTrustor trustor, ToPhase phase, NetDomain domain) {
		array.add(new WorkCommand(trustor, phase, domain));
	}

	/**
	 * 读数据域
	 * @param resp
	 * @return
	 */
	private byte[] readContent(Stream resp) {
		byte[] b = null;
		try {
			b = resp.readContent();
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		return b;
	}
	
//	private void addConductMessage(ToPhase phase, Entity entity) {
//		byte[] s = Apply.build(phase);
//		int limit = 1024;
//		for(int off = 0; off < s.length; ) {
//			int len = (off + limit > s.length ? s.length - off : limit);
//			byte[] b = new byte[len];
//			System.arraycopy(s, off, b, 0, b.length);
//			entity.addMessage(Key.CONDUCT_OBJECT, b);
//			off += len;
//		}
//	}
	
	/**
	 * 组合数据流
	 * @param phase
	 * @param data
	 * @return
	 */
	private byte[] combin(ToPhase phase, NetDomain doamin) throws IOException {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(10240);
		// 串行化ToPhase并且保存
		byte[] s = Apply.build(phase);
		byte[] b = Numeric.toBytes(s.length);
		buff.write(b, 0, b.length);
		buff.write(s, 0, s.length);
		// 串行化分布区域并且保存
		s = Apply.build(doamin);
		b = Numeric.toBytes(s.length);
		buff.write(b, 0, b.length);
		buff.write(s, 0, s.length);
		return buff.toByteArray();
	}

	/**
	 * 执行CONDUCT TCP操作
	 * @param cmd
	 */
	private void conduct_stream(WorkCommand cmd) {
		Command fixpcmd = new Command(Request.SQL, Request.SQL_CONDUCT);
		Stream request = new Stream(fixpcmd);
		
		// 发送数据流并且接收它
		Stream resp = null;
		try {
			byte[] data = this.combin(cmd.phase, cmd.domain);
			request.setData(data, 0, data.length);
			resp = super.executeStream(request, false);
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}

		WorkTrustor finder = cmd.trustor;
		if (resp == null) {
			finder.flushEmpty(this);
		} else {
			fixpcmd = resp.getCommand();
			if(fixpcmd.getResponse() != Response.CONDUCT_OKAY) {
				finder.flushEmpty(this);
			} else {
				// read all data
				byte[] b = readContent(resp);
				if (b == null || b.length == 0) {
					finder.flushEmpty(this);
				} else {
					finder.flushTo(this, 0, b, 0, b.length);
				}
			}
		}
	}
	
	/**
	 * 执行CONDUCT UDP操作
	 * @param cmd
	 */
	private void conduct_packet(WorkCommand cmd) {
		Command command = new Command(Request.SQL, Request.SQL_CONDUCT);
		Packet request = new Packet(command);

		// 发送并且接收数据包
		Packet resp = null;
		try {
			byte[] data = this.combin(cmd.phase, cmd.domain);
			request.setData(data, 0, data.length);
			resp = super.executePacket(request);
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}

		WorkTrustor finder = cmd.trustor;
		if (resp == null) {
			finder.flushEmpty(this);
		} else {
			command = resp.getCommand();
			if (command.getResponse() != Response.CONDUCT_OKAY) {
				finder.flushEmpty(this);
			} else {
				// read all data
				byte[] b = resp.getData();
				if (b == null || b.length == 0) {
					finder.flushEmpty(this);
				} else {
					finder.flushTo(this, 0, b, 0, b.length);
				}
			}
		}
	}
	
	/**
	 * distribute computing
	 * @param cmd
	 */
	private void do_conduct(WorkCommand cmd) {
		if(isStream()) {
			conduct_stream(cmd);
		} else {
			conduct_packet(cmd);
		}
		this.refreshTime();
	}
	
	private void subprocess() {
		while (array.size() > 0) {
			WorkCommand cmd = array.poll();
			if(cmd == null) {
				Logger.fatal("WorkClient.subprocess, null WorkCommand, size:%d", array.size());
				continue;
			}
			
			do_conduct(cmd);
		}
		super.unlock();
	}

	/**
	 * 定时向服务器发送数据包，保持激活状态
	 */
	private boolean active() {
		if(!isLocked()) return false;
		try {
			this.nothing();
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlock();
		}
		return true;
	}
	
	/*
	 * 父类调用这个接口
	 * 
	 * @see com.lexst.remote.client.ThreadClient#execute()
	 */
	@Override
	protected void execute() {
		while (!super.isInterrupted()) {
			if (array.size() > 0) {
				this.subprocess();
			} else {
				if (isRefreshTimeout(20000)) {
					if (!active()) { delay(500); continue;}
				}
				this.delay(2000);
			}
		}
	}

}