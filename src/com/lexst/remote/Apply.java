/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com  All rights reserved
 * 
 * RPC basic class, network request
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

public class Apply implements Serializable {
	private static final long serialVersionUID = 6160007600000448018L;

	/** 调用的接口名 **/
	private String interfaceName;

	/** 接口中的方法名 **/
	private String methodName;

	/** 参数类型 **/
	private Class<?>[] paramTypes;

	/** 实际参数值 **/
	private Object[] params;

	/**
	 * default construct
	 */
	public Apply() {
		super();
	}

	public Apply(Object[] params) {
		this();
		this.params = params;
	}

	public Apply(Object param) {
		this(new Object[] { param });
	}

	/**
	 *
	 * @param interName
	 * @param methodName
	 * @param types
	 * @param params
	 */
	public Apply(String interName, String methodName, Class<?>[] types, Object[] params) {
		this();
		this.interfaceName = interName;
		this.methodName = methodName;
		this.paramTypes = types;
		this.params = params;
	}

	public String getInterfaceName() {
		return this.interfaceName;
	}

	public String getMethodName() {
		return this.methodName;
	}

	/**
	 * 返回参数类型
	 * @return
	 */
	public Class<?>[] getParameterTypes() {
		return this.paramTypes;
	}

	/**
	 * 返回参数集合
	 * @return
	 */
	public Object[] getParameters() {
		return this.params;
	}

	/**
	 * Apply对象串行化生成数据流
	 * @return
	 */
	public byte[] build() throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream(1024 * 5);
		ObjectOutputStream oout = new ObjectOutputStream(bout);
		oout.writeObject(this);
		oout.close();
		return bout.toByteArray();
	}

	/**
	 * Apply对象串行化生成数据流
	 * 
	 * @param object
	 * @return
	 */
	public static byte[] build(Object object) throws IOException {
		Apply apply = new Apply(object);
		return apply.build();
	}

	/**
	 * Apply对象串行化生成数据流
	 * @param objects
	 * @return
	 */
	public static byte[] build(Object[] objects) throws IOException {
		Apply apply = new Apply(objects);
		return apply.build();
	}

	/**
	 * 数据流反串行化为Apply类
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Apply resolve(byte[] b, int off, int len) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bin = new ByteArrayInputStream(b, off, len);
		ObjectInputStream oin = new ObjectInputStream(bin);
		Apply apply = (Apply) oin.readObject();
		oin.close();
		bin.close();
		return apply;
	}
	
	/**
	 * 数据流反串行化为Apply类
	 *
	 * @param b
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Apply resolve(byte[] b) throws IOException, ClassNotFoundException {
		return Apply.resolve(b, 0, b.length);
		// ByteArrayInputStream bin = new ByteArrayInputStream(b);
		// ObjectInputStream oin = new ObjectInputStream(bin);
		// Apply apply = (Apply) oin.readObject();
		// oin.close();
		// bin.close();
		// return apply;
	}


}