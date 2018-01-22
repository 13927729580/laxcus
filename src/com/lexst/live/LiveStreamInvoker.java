/**
 *
 */
package com.lexst.live;

import java.io.*;

import com.lexst.fixp.*;
import com.lexst.invoke.*;
import com.lexst.util.res.*;;

public class LiveStreamInvoker implements StreamInvoker {

	/**
	 *
	 */
	public LiveStreamInvoker() {
		super();
	}

	/**
	 * help document from self
	 * @return
	 */
	public String help() {
		ResourceLoader loader = new ResourceLoader();
		byte[] b = loader.findStream("conf/terminal/ini/help.ini");
		if (b == null || b.length == 0) return "";
		return new String(b, 0, b.length);
	}

	/* (non-Javadoc)
	 * @see com.lexst.invoke.StreamCall#invoke(com.lexst.fixp.Stream, java.io.OutputStream)
	 */
	@Override
	public void invoke(Stream request, OutputStream response)
			throws IOException {
		// TODO Auto-generated method stub

	}

}