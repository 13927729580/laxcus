/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com  All rights reserved
 * 
 * RPC basic class, network response
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 2/1/2009
 * 
 * @see com.lexst.remote
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.remote;

import java.io.*;

/**
 * FIXP应答，执行串行和反串行操作
 */
public class Reply implements Serializable {

	private static final long serialVersionUID = 9173693351345878761L;

	/** 应答结果类 */
	private Object object;

	/** 错误 */
	private Throwable fatal;

	/**
	 * default
	 */
	public Reply() {
		super();
	}

	/**
	 * 
	 * @param object
	 * @param t
	 */
	public Reply(Object object, Throwable t) {
		this();
		this.setObject(object);
		this.setThrowable(t);
	}

	/**
	 * 
	 * @param obj
	 */
	public Reply(Object obj) {
		this();
		this.setObject(obj);
	}

	/**
	 * 返回应答处理结果
	 * 
	 * @return
	 */
	public Object getObject() {
		return this.object;
	}

	/**
	 * 设置应答处理结果
	 * 
	 * @param obj
	 */
	public void setObject(Object obj) {
		this.object = obj;
	}

	/**
	 * 设置处理过程中的错误
	 * 
	 * @param e
	 */
	public void setThrowable(Throwable e) {
		this.fatal = e;
	}

	/**
	 * 返回处理过程中的错误
	 * 
	 * @return
	 */
	public Throwable getThrowable() {
		return this.fatal;
	}

	/**
	 * 返回错误堆栈信息
	 * 
	 * @return
	 */
	public String getThrowText() {
		return Reply.getMessage(fatal);
	}

	/**
	 * 返回错误信息堆栈
	 * 
	 * @param e
	 * @return
	 */
	public static String getMessage(Throwable e) {
		if (e == null)
			return "";
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		PrintStream s = new PrintStream(out, true);
		e.printStackTrace(s);
		byte[] data = out.toByteArray();
		return new String(data, 0, data.length);
	}

	/**
	 * Reply类串行化为字节流
	 * 
	 * @return
	 */
	public byte[] build() throws IOException {
		ByteArrayOutputStream a = new ByteArrayOutputStream(1024);
		ObjectOutputStream o = new ObjectOutputStream(a);
		o.writeObject(this);
		o.close();
		return a.toByteArray();
	}

	/**
	 * Reply类串行化为字节流
	 * 
	 * @param object
	 * @return
	 */
	public static byte[] build(Object object) throws IOException {
		Reply reply = new Reply(object);
		return reply.build();
	}

	/**
	 * 数据流反串行化为Reply类
	 * 
	 * @param b
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Reply resolve(byte[] b, int off, int len) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bi = new ByteArrayInputStream(b, off, len);
		ObjectInputStream oi = new ObjectInputStream(bi);
		Reply reply = (Reply) oi.readObject();
		oi.close();
		bi.close();
		return reply;
	}

	/**
	 * 数据流反串行化为Reply类
	 * 
	 * @param b
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Reply resolve(byte[] b) throws IOException, ClassNotFoundException {
		return Reply.resolve(b, 0, b.length);
	}
}
