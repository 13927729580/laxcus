/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * class or resource
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 10/19/2010
 * 
 * @see com.lexst.algorithm
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.algorithm;

import java.io.Serializable;


final class DataEntry implements Serializable{
	
	private static final long serialVersionUID = -3368877258392891746L;

	/* class name or resource url */
	private String name;
	
	private String jarurl;
	
	private byte[] data;

	/**
	 * default
	 */
	public DataEntry() {
		super();
	}
	
	/**
	 * @param name
	 * @param b
	 */
	public DataEntry(String name, byte[] b) {
		this();
		this.name = name;
		if (b != null && b.length > 0) {
			data = new byte[b.length];
			System.arraycopy(b, 0, data, 0, b.length);
		}
	}

	/**
	 * @param name
	 */
	public DataEntry(String name, String url, byte[] b) {
		this(name, b);
		this.jarurl = url;
	}

	/**
	 * class name or resource name
	 * @return
	 */
	public String getName() {
		return this.name;
	}
	
	/**
	 * resource jar url
	 * @return
	 */
	public String getURL() {
		return this.jarurl;
	}
	
	/**
	 * binary data
	 * @return
	 */
	public byte[] getData() {
		return this.data;
	}
}