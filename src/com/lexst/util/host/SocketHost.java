/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * socket address (ip address, tcp port, udp port)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 10/07/2009
 * 
 * @see com.lexst.util.host
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.util.host;

import java.io.*;
import java.net.*;
import java.util.regex.*;

/**
 * 客户端本地绑定地址或者对方地址，分为TCP/UDP两种模式。<br><br>
 * 
 * 适用于环境所有条件。<br>
 */
public final class SocketHost implements Serializable, Cloneable, Comparable<SocketHost> {

	private static final long serialVersionUID = 6731177355531664038L;

	/** 正则表达式 */
	private final static String REGEX = "^\\s*(?i)(?:NONE|TCP|UDP)://([0-9a-fA-F.:]{1,}):([0-9]{1,})\\s*$";

	/** SOCKET连接类型 */
	public final static int NONE = 0;

	public final static int UDP = 1;

	public final static int TCP = 2;

	/** 当前SOCKET连接类型 */
	private int family;

	/** SOCKT网络地址 **/
	private Address address;

	/** SOCKET端口号(TCP/UDP端口) */
	private int port;

	/**
	 * construct method
	 */
	protected SocketHost() {
		super();
		family = SocketHost.NONE;
		address = new Address();
		port = 0;
	}

	/**
	 * construct method
	 * 
	 * @param family
	 */
	public SocketHost(int family) {
		this();
		this.setFamily(family);
	}

	/**
	 * @param family
	 * @param address
	 * @param port
	 * @throws UnknownHostException
	 */
	public SocketHost(int family, byte[] address, int port) throws UnknownHostException {
		this(family);
		this.address.setAddress(address);
		this.setPort(port);
	}

	/**
	 * @param family
	 * @param address
	 * @param port
	 */
	public SocketHost(int family, InetAddress address, int port) {
		this(family);
		this.address.setAddress(address);
		this.setPort(port);
	}

	/**
	 * 
	 * @param family
	 * @param address
	 * @param port
	 * @throws UnknownHostException
	 */
	public SocketHost(int family, String address, int port) throws UnknownHostException {
		this(family, InetAddress.getByName(address), port);
	}

	/**
	 * @param address - 域名主机或者IP地址
	 * @param port
	 * @throws UnknownHostException
	 */
	public SocketHost(String address, int port) throws UnknownHostException {
		this(SocketHost.NONE, InetAddress.getByName(address), port);
	}

	/**
	 * 创建对象，使用正则表达式解析主机地址参数序列。
	 * 
	 * @param input - 正则表达式解析的URI格式
	 * @throws UnknownHostException
	 */
	public SocketHost(String input) throws UnknownHostException {
		this();
		this.resolve(input);
	}
	
	/**
	 * 创建新对象，使用传入对象相同的参数(建立副本)。
	 * 
	 * @param host
	 */
	public SocketHost(SocketHost host) {
		this();
		this.set(host);
	}

	/**
	 * SOCKET连接类型(TCP/UDP)
	 * 
	 * @param i
	 */
	public void setFamily(int i) {
		if(!(SocketHost.NONE <= family && family <= SocketHost.TCP)) {
			throw new IllegalArgumentException("invalid host family!");
		}
		this.family = i;
	}

	/**
	 * 返回SOCKET连接类型
	 * @return
	 */
	public int getFamily() {
		return this.family;
	}

	/**
	 * 设置SOCKET地址
	 * @param host
	 */
	public void set(SocketHost host) {
		this.setFamily(host.family);
		this.setAddress(host.address);
		this.setPort(host.port);
	}
	
	/**
	 * 设置INTERNET网络地址
	 * @param inet
	 */
	public void setInetAddress(InetAddress inet) {
		this.address.setAddress(inet);
	}
	
	/**
	 * 返回INTERNET网络地址
	 * @return
	 */
	public InetAddress getInetAddress() {
		return this.address.getAddress();
	}
	
	/**
	 * 设置网络地址
	 * @param address
	 */
	public void setAddress(Address address) {
		this.address.set(address);
	}

	/**
	 * 返回网络地址
	 * @return
	 */
	public Address getAddress() {
		return this.address;
	}

	/**
	 * 返回二进制IP地址描述 
	 * @return
	 */
	public byte[] getRawAddress() {
		return this.address.bits();
	}
	
	/**
	 * 返回十进制/十六进制IP地址描述
	 * @return
	 */
	public String getSpecifyAddress() {
		return this.address.getSpecification();
	}
	
	/**
	 * 判断地址是否有效
	 * @return
	 */
	public boolean isValid() {
		return !address.isAnyLocalAddress() && port > 0;
	}

	/**
	 * set socket host port
	 * 
	 * @param i
	 */
	public void setPort(int i) {
		if (i < 0 || i >= 0xFFFF) {
			throw new IllegalArgumentException("invalid host port, must >0 && <0xFFFF");
		}
		this.port = i;
	}

	/**
	 * return socket host port
	 * 
	 * @return int
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * @param input
	 */
	public void resolve(String input) throws UnknownHostException {
		Pattern pattern = Pattern.compile(SocketHost.REGEX);
		Matcher matcher = pattern.matcher(input);
		if (!matcher.matches()) {
			throw new UnknownHostException("invalid socket address:" + input);
		}

		String s1 = matcher.group(1);

		// 协议族
		if ("TCP".equalsIgnoreCase(s1)) {
			family = SocketHost.TCP;
		} else if ("UDP".equalsIgnoreCase(s1)) {
			family = SocketHost.UDP;
		} else if ("NONE".equalsIgnoreCase(s1)) {
			family = SocketHost.NONE;
		}
		// 解析IP地址
		address.resolve(matcher.group(2));
		// 端口号
		port = Integer.parseInt(matcher.group(3));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		String s = "NONE";
		switch (family) {
		case SocketHost.TCP:
			s = "TCP";
			break;
		case SocketHost.UDP:
			s = "UDP";
			break;
		}
		return String.format("%s://%s:%d", s, address.getSpecification(), port); 
	}

	/**
	 * 
	 * @return
	 */
	public InetSocketAddress getSocketAddress() {
		return new InetSocketAddress(address.getAddress(), this.port);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != SocketHost.class) {
			return false;
		} else if (object == this) {
			return true;
		}

		return this.compareTo((SocketHost) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return address.hashCode() ^ family ^ port;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new SocketHost(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(SocketHost host) {
		if (host == null) return -1;

		int ret = (family < host.family ? -1 : (family > host.family ? 1 : 0));
		if (ret == 0) {
			ret = address.compareTo(host.address);
		}
		if (ret == 0) {
			ret = (port < host.port ? -1 : (port > host.port ? 1 : 0));
		}
		return ret;
	}
}