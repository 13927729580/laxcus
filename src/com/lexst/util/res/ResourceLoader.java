/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2012 lexst.com. All rights reserved
 * 
 * extract resource (document, image, video, audio) from jar file
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 12/23/2012
 * 
 * @see com.lexst.live.util
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.util.res;

import java.awt.*;
import java.io.*;
import java.net.URL;

import javax.swing.*;

public class ResourceLoader {

	/* resource root directory, eg:"conf/terminal/" */
	private String root;

	/**
	 * 
	 */
	public ResourceLoader() {
		super();
		root = "";
	}

	/**
	 * set root name
	 * 
	 * @param root
	 */
	public ResourceLoader(String root) {
		this();
		this.setRoot(root);
	}

	/**
	 * set resource root of jar file
	 * @param name
	 */
	public void setRoot(String name) {
		if (name.charAt(name.length() - 1) != '/') {
			name += "/";
		}
		root = name;
	}

	/**
	 * resource root
	 * @return
	 */
	public String getRoot() {
		return root;
	}
	
	/**
	 * resource url address, from jar file
	 * eg: <jar:file:/E:/lexst/lib/spider.jar!/com/lexst/spider/app.gif>
	 * @param name
	 * @return
	 */
	public URL findURL(String name) {
		String path = root + name;
		ClassLoader loader = ClassLoader.getSystemClassLoader();
		return loader.getResource(path);
	}

	/**
	 * find resource data from jar file
	 * @param name
	 * @return
	 */
	public byte[] findStream(String name) {
		String path = root + name;
		ClassLoader loader = ClassLoader.getSystemClassLoader();
		InputStream in = loader.getResourceAsStream(path);
		if(in == null) return null;
		
		byte[] b = new byte[1024];
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		try {
			while (true) {
				int len = in.read(b, 0, b.length);
				if (len == -1) break;
				buff.write(b, 0, len);
			}
			in.close();
		} catch (IOException exp) {
			return null;
		}

		if (buff.size() == 0) return null;
		return buff.toByteArray();
	}

	/**
	 * extract image from jar file
	 * 
	 * @param name
	 * @return
	 */
	public ImageIcon findImage(String name) {
		byte[] b = findStream(name);
		if(b == null) return null;
		return new ImageIcon(b);
	}

	/**
	 * extract image from jar file
	 * @param name
	 * @param width
	 * @param height
	 * @return
	 */
	public ImageIcon findImage(String name, int width, int height) {
		ImageIcon icon = findImage(name);
		if (icon != null) {
			Image img = icon.getImage().getScaledInstance(width, height,Image.SCALE_SMOOTH);
			return new ImageIcon(img);
		}
		return null;
	}
}