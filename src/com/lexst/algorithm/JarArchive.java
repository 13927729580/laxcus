/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * jar file property
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

import java.io.*;
import java.util.*;

import com.lexst.util.naming.*;

final class JarArchive implements Serializable {

	private static final long serialVersionUID = 6262807441719907816L;

	private String filename;

	private long lastime;

	private long length;

	private List<Naming> names = new ArrayList<Naming>();
	
	/* class name | resource name - data entry */
	private Map<String, DataEntry> entries = new HashMap<String, DataEntry>();

	// tasks.xml content
	private byte[] taskText;
	
	/**
	 * 
	 */
	public JarArchive() {
		super();
		filename = "";
	}

	/**
	 * @param filename
	 * @param len
	 * @param time
	 */
	public JarArchive(String filename, long len, long time) {
		this();
		this.filename = new String(filename);
		this.length = len;
		this.lastime = time;
	}

	public String getFilename() {
		return this.filename;
	}

	public void setFilename(String s) {
		this.filename = s;
	}

	public long getLength() {
		return this.length;
	}

	public void setLength(long len) {
		this.length = len;
	}

	public long getLastTime() {
		return this.lastime;
	}

	public void setLastTime(long time) {
		this.lastime = time;
	}
	
	public void addNaming(Naming s) {
		this.names.add(s);
	}
	
	public List<Naming> listNaming() {
		return names;
	}
	
	public void addEntry(DataEntry entry) {
		entries.put(entry.getName(), entry);
	}
	
	public Map<String, DataEntry> entrys() {
		return this.entries;
	}
	
	/**
	 * text's tasks.xml
	 * @param b
	 */
	public void setTaskText(byte[] b) {
		taskText = new byte[b.length];
		System.arraycopy(b, 0, taskText, 0, b.length);
	}

	public byte[] getTaskText() {
		return this.taskText;
	}

	/**
	 * same file
	 * @param ja
	 * @return
	 */
	public boolean match(JarArchive ja) {
		return filename.equals(ja.filename) && length == ja.length
				&& lastime == ja.lastime;
	}

	/*
	 * check filename
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != JarArchive.class) return false;
		JarArchive ja = (JarArchive) arg;
		return filename.equals(ja.filename);
	}

	/*
	 * filename hash code
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return filename.hashCode();
	}
}