/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * class loader, from URLClassLoader
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

import java.net.*;
import java.util.*;
import java.io.*;

public class TaskClassLoader extends URLClassLoader {
	
	/** class or resource's name -> binary array */
	private Map<String, DataEntry> entries;
	
	/**
	 * default function
	 */
	public TaskClassLoader() {
		super(new URL[0], ClassLoader.getSystemClassLoader());
	}
	
	/**
	 * set map 
	 * @param map
	 */
	public TaskClassLoader(Map<String, DataEntry> map) {
		this();
		this.entries = map;
	}

	/*
	 * 根据名称查找对应类
	 * @see java.net.URLClassLoader#findClass(java.lang.String)
	 */
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		if (entries != null) {
			DataEntry elem = entries.get(name);
			if (elem != null) {
				byte[] b = elem.getData();
				return super.defineClass(name, b, 0, b.length);
			}
		}
		return super.findClass(name);
	}

	/*
	 * 根据名称查找资源配置
	 * @see java.net.URLClassLoader#findResource(java.lang.String)
	 */
	public URL findResource(String name) {
		if (entries != null) {
			DataEntry elem = entries.get(name);
			if (elem != null) {
				try {
					return new URL(elem.getURL());
				} catch (MalformedURLException exp) {
					return null;
				}
			}
		}
		return super.findResource(name);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.ClassLoader#getResourceAsStream(java.lang.String)
	 */
	public InputStream getResourceAsStream(String name) {
		if (entries != null) {
			DataEntry elem = entries.get(name);
			if (elem != null) {
				byte[] b = elem.getData();
				return new ByteArrayInputStream(b);
			}
		}
		return super.getResourceAsStream(name);
	}
	
//	/*
//	 * load a class from memory or super class
//	 * @see java.lang.ClassLoader#loadClass(java.lang.String, boolean)
//	 */
//	protected Class<?> loadClass(String name, boolean resolve)
//			throws ClassNotFoundException {
//
//		if (entrys != null) {
//			ClassEntry ce = entrys.get(name);
//			if (ce != null) {
//				byte[] b = ce.getData();
//				return super.defineClass(name, b, 0, b.length);
//			}
//		}
//
//		return super.loadClass(name, resolve);
//	}
	
//	/**
//	 * @param filename
//	 */
//	public boolean addJar(String filename) {
//		File file = new File(filename);
//		if(!file.exists()) return false;
//		
//		Logger.info("TaskClassLoader.add, load '%s'", filename);
//		
//		try {
//			super.addURL(file.toURI().toURL());
//			return true;
//		} catch (MalformedURLException exp) {
//			Logger.error(exp);
//		}
//		return false;
//	}
	
//	private byte[] loadClassData(String name) {
//		System.out.printf("find class binary:%s\n", name);
//		
//		String s = "E:/workspace/tool/bin/org/lexst/find/Finder.class";
//		File file = new File(s);
//		byte[] b = new byte[(int)file.length()];
//		try {
//		java.io.FileInputStream in = new java.io.FileInputStream(file);
//		in.read(b, 0, b.length);
//		in.close();
//		} catch (IOException exp) {
//			exp.printStackTrace();
//		}
//		return b;
//	}
	
//	protected Class<?> loadClass(String name, boolean resolve)
//			throws ClassNotFoundException {
//
//		System.out.printf("in TaskClassLoader.loadClass method, name is:%s\n", name);
//
//		if (name.equalsIgnoreCase("org.lexst.find.Finder")) {
//			byte[] b = this.loadClassData(name);
//			return super.defineClass(name, b, 0, b.length);
//		} else {
//			return super.loadClass(name, resolve);
//		}
//	}

	
	// below is test code
//	protected Class<?> findClass(String name) throws ClassNotFoundException {
//		System.out.println("in TaskClassLoader.findClass method");
//		
//		byte[] b = this.loadClassData(name);
//		return super.defineClass(name, b, 0, b.length);
//	}
//
//	private byte[] loadClassData(String name) {
//		System.out.printf("find class binary:%s\n", name);
//		
//		String s = "E:/workspace/tool/bin/org/lexst/find/Finder.class";
//		File file = new File(s);
//		byte[] b = new byte[(int)file.length()];
//		try {
//		java.io.FileInputStream in = new java.io.FileInputStream(file);
//		in.read(b, 0, b.length);
//		in.close();
//		} catch (IOException exp) {
//			exp.printStackTrace();
//		}
//		return b;
//	}
//	
//	protected Class<?> loadClass(String name, boolean resolve)
//			throws ClassNotFoundException {
//
//		System.out.printf("in TaskClassLoader.loadClass method, name is:%s\n", name);
//
//		if (name.equalsIgnoreCase("org.lexst.find.Finder")) {
//			byte[] b = this.loadClassData(name);
//			return super.defineClass(name, b, 0, b.length);
//		} else {
//			return super.loadClass(name, resolve);
//		}
//	}
}