/**
 * 
 */
package com.lexst.work;

import java.io.*;

import com.lexst.fixp.*;
import com.lexst.invoke.*;
import com.lexst.util.host.*;
import com.lexst.work.pool.*;

/**
 * WORK节点数据流服务接收器。<br>
 * 目前数据流服务接收器只受理conduct命令。<br>
 *
 */
public class WorkStreamInvoker implements StreamInvoker {

	/**
	 * default constructor
	 */
	public WorkStreamInvoker() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.invoke.StreamInvoker#invoke(com.lexst.fixp.Stream,
	 * java.io.OutputStream)
	 */
	@Override
	public void invoke(Stream request, OutputStream output) throws IOException {
		Stream resp = null;

		Command cmd = request.getCommand();
		byte major = cmd.getMajor();
		byte minor = cmd.getMinor();
		
		if (major == Request.SQL && minor == Request.SQL_CONDUCT) {
			resp = conduct(request);
		}

		if (resp != null) {
			byte[] b = resp.build();
			output.write(b, 0, b.length);
			output.flush();
		}
	}

	/**
	 * 执行数据流式的AGGREGATE阶段的分布计算，AGGREGATE阶段允许有多个子阶段。<br>
	 * 
	 * @param request
	 * @return
	 * @throws IOException
	 */
	private Stream conduct(Stream request) throws IOException {
		SocketHost remote = request.getRemote();
		byte[] data = request.readContent();
		Stream resp = (Stream) ConductPool.getInstance().conduct(data, 0, data.length, true);
		resp.setRemote(remote);
		return resp;
	}
}