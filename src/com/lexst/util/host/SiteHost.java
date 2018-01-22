/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * site address (tcp and udp mode)
 * 
 * @author scott.liang laxcus@126.com
 * 
 * @version 1.0 10/07/2009
 * 
 * @see com.lexst.util.host
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.util.host;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

import com.lexst.util.*;

/**
 * 节点服务器主机绑定地址描述，包括IP地址、TCP端口号、UDP端口号<br>
 * <br>
 * 适用本环境中所有节点。<br>
 */
public final class SiteHost implements Serializable, Cloneable, Comparable<SiteHost> {

	private static final long serialVersionUID = 2877957652797182372L;

	/** SiteHost的正则表达式 */
	private final static String REGEX = "^\\s*(?i)(?:SITE)://([0-9a-fA-F.:]{1,}):([0-9]{1,})_([0-9]{1,})\\s*$";

	/** SOCKT的网络地址 **/
	private Address address;

	/** SOCKET的TCP/UDP端口 **/
	private int udport, tcport;

	/**
	 * default
	 */
	public SiteHost() {
		super();
		address = new Address();
		udport = tcport = 0;
	}

	/**
	 * 
	 * @param address
	 * @param tcport
	 * @param udport
	 */
	public SiteHost(InetAddress address, int tcport, int udport) {
		this();
		this.set(address, tcport, udport);
	}

	/**
	 * @param address
	 * @param tcport
	 * @param udport
	 * @throws UnknownHostException
	 */
	public SiteHost(byte[] address, int tcport, int udport) throws UnknownHostException {
		this(InetAddress.getByAddress(address), tcport, udport);
	}
	
	/**
	 * @param address
	 * @param tcport
	 * @param udport
	 */
	public SiteHost(Address address, int tcport, int udport) {
		this(address.getAddress(), tcport, udport);
	}

	/**
	 * @param address
	 * @param tcport
	 * @param udport
	 * @throws UnknownHostException
	 */
	public SiteHost(String address, int tcport, int udport) throws UnknownHostException {
		this(InetAddress.getByName(address), tcport, udport);
	}

	/**
	 * 创建新对象，使用正则表达式解析节点主机参数序列
	 * 
	 * @param input - 正则表达式解析值
	 */
	public SiteHost(String input) throws UnknownHostException {
		this();
		this.resolve(input);
	}

	/**
	 * 创建新对象，使用传入对象相同的参数(建立副本)。
	 * 
	 * @param site
	 */
	public SiteHost(SiteHost site) {
		this();
		this.set(site);
	}

	/**
	 * 设置节点主机地址
	 * 
	 * @param address
	 * @param tcport
	 * @param udport
	 */
	public void set(InetAddress address, int tcport, int udport) {
		this.setInetAddress(address);
		this.setTCPort(tcport);
		this.setUDPort(udport);
	}

	/**
	 * 设置节点地址
	 * 
	 * @param site
	 */
	public void set(SiteHost site) {
		this.setAddress(site.address);
		this.setTCPort(site.tcport);
		this.setUDPort(site.udport);
	}

	/**
	 * 设置INTERNET网络地址
	 * 
	 * @param address
	 */
	public void setInetAddress(InetAddress address) {
		this.address.setAddress(address);
	}

	/**
	 * 返回INTERNET网络地址
	 * 
	 * @return
	 */
	public InetAddress getInetAddress() {
		return this.address.getAddress();
	}

	/**
	 * 设置网络地址
	 * 
	 * @param address
	 */
	public void setAddress(Address address) {
		this.address.set(address);
	}

	/**
	 * 返回网络地址
	 * 
	 * @return
	 */
	public Address getAddress() {
		return this.address;
	}

	/**
	 * 返回二进制IP地址描述
	 * 
	 * @return
	 */
	public byte[] getRawAddress() {
		return this.address.bits();
	}

	/**
	 * 返回十进制/十六进制IP地址描述
	 * 
	 * @return
	 */
	public String getSpecifyAddress() {
		return this.address.getSpecification();
	}

	/**
	 * 设置TCP端口
	 * 
	 * @param port
	 */
	public void setTCPort(int port) {
		if (port >= 0) this.tcport = port;
	}

	/**
	 * 返回TCP端口
	 * 
	 * @return
	 */
	public int getTCPort() {
		return this.tcport;
	}

	/**
	 * 设置UDP端口
	 * 
	 * @param port
	 */
	public void setUDPort(int port) {
		if (port >= 0) this.udport = port;
	}

	/**
	 * 返回UDP端口
	 * 
	 * @return
	 */
	public int getUDPort() {
		return this.udport;
	}

	/**
	 * 返回流模式套接字
	 * 
	 * @return
	 */
	public SocketHost getStreamHost() {
		return new SocketHost(SocketHost.TCP, address.getAddress(), tcport);
	}

	/**
	 * 返回包模式套接字
	 * 
	 * @return
	 */
	public SocketHost getPacketHost() {
		return new SocketHost(SocketHost.UDP, address.getAddress(), udport);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.util.host.Address#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != SiteHost.class) {
			return false;
		} else if (object == this) {
			return true;
		}

		return this.compareTo((SiteHost) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.util.host.Address#hashCode()
	 */
	@Override
	public int hashCode() {
		return address.hashCode() ^ tcport ^ udport;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(SiteHost site) {
		if (site == null) return -1;

		int ret = address.compareTo(site.address);
		if (ret == 0) {
			ret = (tcport < site.tcport ? -1 : (tcport > site.tcport ? 1 : 0));
		}
		if (ret == 0) {
			ret = (udport < site.udport ? -1 : (udport > site.udport ? 1 : 0));
		}
		return ret;
	}

	/**
	 * 解析节点地址
	 * 
	 * @param input
	 * @throws UnknownHostException
	 */
	public void resolve(String input) throws UnknownHostException {
		Pattern pattern = Pattern.compile(SiteHost.REGEX);
		Matcher matcher = pattern.matcher(input);
		if (!matcher.matches()) {
			throw new UnknownHostException("invalid site address!" + input);
		}

		// 解析IP地址
		address.resolve(matcher.group(1));
		// TCP/UDP端口号
		tcport = Integer.parseInt(matcher.group(2));
		udport = Integer.parseInt(matcher.group(3));
	}

	/*
	 * show site address
	 * 
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return String.format("SITE://%s:%d_%d", address.getSpecification(), tcport, udport);
	}

	/*
	 * 克隆节点主机地址
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new SiteHost(this);
	}
	
	/**
	 * 节点数据转换为字节流
	 * @return
	 */
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(32);

		byte[] b = address.bits(); 
		buff.write((byte) (b.length));
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(this.tcport);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(this.udport);
		buff.write(b, 0, b.length);

		return buff.toByteArray();
	}
	
	/**
	 * 解析节点数据，返回解析长度
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		// 网络地址字节长
		int size = b[seek] & 0xFF;
		seek += 1;
		if (size != 4 && size != 16) {
			throw new SizeOutOfBoundsException("site address error!");
		}
		// 检查是否超出范围
		if (seek + size + 8 > end) {
			throw new SizeOutOfBoundsException("site size missing!");
		}

		// 读取并且设置节点数据
		try {
			address.setAddress(Arrays.copyOfRange(b, seek, seek + size));
			seek += size;
		} catch (UnknownHostException e) {
			throw new SizeOutOfBoundsException(e);
		}
		this.tcport = Numeric.toInteger(b, seek, 4);
		seek += 4;
		this.udport = Numeric.toInteger(b, seek, 4);
		seek += 4;

		return seek - off;
	}
}