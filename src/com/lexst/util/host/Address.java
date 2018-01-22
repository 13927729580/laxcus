/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.util.host;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

/**
 * TCP/IP网络IPv6(128位)地址，兼容IPv4。<br><br>
 * 
 * 适用当所环境所有条件。<br>
 */
public final class Address implements Serializable, Cloneable, Comparable<Address> {

	private static final long serialVersionUID = 4220709644122777607L;

	/** IPv4 和 IPv6的网络地址正则表达式 **/
	private final static String REGEX_IPV4 = "^\\s*([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})\\s*$";

	private final static String REGEX_IPV6 = "^\\s*(\\p{XDigit}{1,4}):(\\p{XDigit}{1,4}):(\\p{XDigit}{1,4}):(\\p{XDigit}{1,4}):(\\p{XDigit}{1,4}):(\\p{XDigit}{1,4}):(\\p{XDigit}{1,4}):(\\p{XDigit}{1,4})\\s*$";

	/** TCP/IP v6 网络地址(128位)，兼容IPv4地址 **/
	private long high, low;

	/** 哈希码 */
	private int hash;

	/**
	 * 初始化地址，默认为IPv4自回路地址
	 */
	protected Address() {
		super();
		this.high = 0L;
		this.low = 0L;
		this.hash = 0;
	}
	
	/**
	 * 分配一个INETNERT网络地址
	 * @param address
	 */
	public Address(InetAddress address) {
		this();
		this.setAddress(address);
	}
	
	/**
	 * 通过原始IP地址字节数组构造一个新对象
	 * @param addr
	 * @throws UnknownHostException
	 */
	public Address(byte[] addr) throws UnknownHostException {
		this();
		this.setAddress(addr);
	}

	/**
	 * 通过字符串描述的网络地址构造一个新对象
	 * 
	 * @param input - 字符串描述的IP地址或者域名主机地址
	 * @throws UnknownHostException
	 */
	public Address(String input) throws UnknownHostException {
		this();
		this.setAddress(input);
	}

	/**
	 * 新创建一个对象，并且使用同参数值(创建副本)。
	 * 
	 * @param address
	 */
	public Address(Address address) {
		this();
		this.set(address);
	}

	/**
	 * 128位的高位地址序列
	 * 
	 * @return
	 */
	protected long high() {
		return this.high;
	}

	/**
	 * 128位的低位地址序列
	 * 
	 * @return
	 */
	protected long low() {
		return this.low;
	}
	
//	protected void set(long h, int l) {
//		this.high = h;
//		this.low = l;
//	}

	/**
	 * 设置网络地址
	 * @param address
	 */
	public void set(Address address) {
		this.high = address.high;
		this.low = address.low;
		this.hash = address.hash;
	}

	/**
	 * 设置TCP/IP网络地址的字节序列
	 * 
	 * @param b
	 */
	public void setAddress(byte[] b) throws UnknownHostException {
		int index = 0;
		long high64 = 0L, low64 = 0L;
		// 字节排序是高位在前低位在后
		if (b.length == 4) {
			for (int seek = 24; seek >= 0; seek -= 8) {
				long value = b[index++] & 0xFF;
				low64 |= (value << seek);
			}
			// 设置哈希码，最高位是0代表是IPV4地址
			hash = (int) (low64 & 0x7FFFFFFFL);
		} else if (b.length == 16) {
			for (int seek = 56; seek >= 0; seek -= 8) {
				long value = b[index++] & 0xFF;
				high64 |= (value << seek);
			}
			for (int seek = 56; seek >= 0; seek -= 8) {
				long value = b[index++] & 0xFF;
				low64 |= (value << seek);
			}
			// 设置哈希码，最高位是1代表IPV6地址
			hash = 1;
			hash <<= 31;
			hash |= (int) ((high64 ^ low64) & 0x7FFFFFFFL);
		} else {
			throw new UnknownHostException("error raw address!");
		}

		// 赋值
		this.high = high64;
		this.low = low64;
	}

	/**
	 * 设置TCP/IP网络地址
	 * 
	 * @param address
	 */
	public void setAddress(InetAddress address) {
		try {
			this.setAddress(address.getAddress());
		} catch (UnknownHostException e) {

		}
	}

	/**
	 * 根据主机域名或者主机IP地址，设置TCP/IP网络地址
	 * 
	 * @param input - 主机域名或者IP地址
	 * @throws UnknownHostException
	 */
	public void setAddress(String input) throws UnknownHostException {
		if (input.matches(Address.REGEX_IPV4) || input.matches(Address.REGEX_IPV6)) {
			this.resolve(input);
		} else {
			this.setAddress(InetAddress.getByName(input));
		}
	}

	/**
	 * 返回TCP/IP网络地址
	 * 
	 * @return
	 */
	public InetAddress getAddress() {
		byte[] b = bits();
		try {
			return InetAddress.getByAddress(b);
		} catch (UnknownHostException e) {

		}
		return null;
	}

	/**
	 * 返回TCP/IP网络地址的字节序列
	 * 
	 * @return
	 */
	public byte[] bits() {
		byte[] b = new byte[isIPv4() ? 4 : 16];
		int index = 0;
		if (b.length == 4) {
			for (int seek = 24; seek >= 0; seek -= 8) {
				b[index++] = (byte) ((low >>> seek) & 0xFF);
			}
		} else {
			for (int seek = 56; seek >= 0; seek -= 8) {
				b[index++] = (byte) ((high >>> seek) & 0xFF);
			}
			for (int seek = 56; seek >= 0; seek -= 8) {
				b[index++] = (byte) ((low >>> seek) & 0xFF);
			}
		}
		return b;
	}

	/**
	 * 返回IP地址的十进制/十六进制字符串格式
	 * 
	 * @return
	 */
	public String getSpecification() {
		return this.toString();
	}

	/**
	 * 哈希码最高位是0代表IPv4地址
	 * 
	 * @return
	 */
	public boolean isIPv4() {
		return (hash >>> 31) == 0;
	}

	/**
	 * 哈希码最高位是1代表IPv6地址
	 * 
	 * @return
	 */
	public boolean isIPv6() {
		return (hash >>> 31) == 1;
	}

	/**
	 * 判断是不是通配符地址(全0)
	 * 
	 * @return
	 */
	public boolean isAnyLocalAddress() {
		return getAddress().isAnyLocalAddress();
	}

	/**
	 * 判断是不是自回路地址(127.0.0.1)
	 * 
	 * @return
	 */
	public boolean isLoopbackAddress() {
		return getAddress().isLoopbackAddress();
	}
	
	/**
	 * 判断是不是设备自动配置地址(169.254.xxx.xxx)
	 * 
	 * @return
	 */
	public boolean isLinkLocalAddress() {
		return getAddress().isLinkLocalAddress();
	}
	
	/**
	 * 判断是不是广播地址(224.xxx.xxx.xxx)
	 * 
	 * @return
	 */
	public boolean isMulticastAddress() {
		return getAddress().isMulticastAddress();
	}
	
	/**
	 * 判断是不是内网地址
	 * @return
	 */
	public boolean isSiteLocalAddress() {
		return getAddress().isSiteLocalAddress();
	}
	
	/**
	 * 比较在输入的地址数组里，是否有匹配的存在
	 * 
	 * @param inputs
	 * @return
	 */
	public boolean matchsIn(Address[] inputs) {
		for(int i = 0; inputs != null && i < inputs.length; i++) {
			if (this.compareTo(inputs[i]) == 0) return true;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != Address.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((Address) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.hash;
	}

	/*
	 * 返回TCP/IP网络地址的字符串格式
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder buff = new StringBuilder();
		if (isIPv4()) {
			for (int seek = 24; seek >= 0; seek -= 8) {
				if (buff.length() > 0) buff.append(".");
				buff.append(String.format("%d", ((low >> seek) & 0xFFL)));
			}
		} else {
			for (int seek = 48; seek >= 0; seek -= 16) {
				if (buff.length() > 0) buff.append(":");
				buff.append(String.format("%X", ((high >> seek) & 0xFFFFL)));
			}
			for (int seek = 48; seek >= 0; seek -= 16) {
				if (buff.length() > 0) buff.append(":");
				buff.append(String.format("%X", ((low >> seek) & 0xFFFFL)));
			}
		}
		return buff.toString();
	}
	
	/*
	 * 复制一个新的对象
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Address(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Address address) {
		int ret = (high == address.high ? 0 : (high < address.high ? -1 : 1));
		if (ret == 0) {
			ret = (low < address.low ? -1 : (low > address.low ? 1 : 0));
		}
		return ret;
	}
	
	/**
	 * 解析TCP/IP网络地址
	 * 
	 * @param input
	 * @throws UnknownHostException
	 */
	public void resolve(String input) throws UnknownHostException {
		// IPv4地址格式
		Pattern pattern = Pattern.compile(Address.REGEX_IPV4);
		Matcher matcher = pattern.matcher(input);
		if (matcher.matches()) {
			byte[] b = new byte[4];
			for (int i = 0; i < b.length; i++) {
				String s = matcher.group(i + 1);
				int value = Integer.parseInt(s);
				if (value > 0xFF) {
					throw new UnknownHostException("ip address out!");
				}
				b[i] = (byte) (value & 0xFF);
			}
			this.setAddress(b);
			return;
		}
		// IPv6地址格式
		pattern = Pattern.compile(Address.REGEX_IPV6);
		matcher = pattern.matcher(input);
		if (matcher.matches()) {
			byte[] b = new byte[16];
			int seek = 0;
			for (int index = 1; index <= 8; index++) {
				String s = matcher.group(index);
				int value = Integer.parseInt(s, 16) & 0xFFFF;
				b[seek++] = (byte) ((value >>> 8) & 0xFF);
				b[seek++] = (byte) (value & 0xFF);
			}
			this.setAddress(b);
			return;
		}
		// 错误!
		throw new UnknownHostException("cannot resolve address!");
	}
	
	/**
	 * 统计当前主机绑定的所有TCP/IP网络地址并且返回地址集合
	 * 
	 * @return
	 */
	public static Address[] locales() {
		ArrayList<Address> a = new ArrayList<Address>();

		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()) {
				Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress inet = addresses.nextElement();

					a.add(new Address(inet));
				}
			}
		} catch (SocketException exp) {

		}

		Address[] s = new Address[a.size()];
		return a.toArray(s);
	}

	/**
	 * 枚举环境中的网络地址，返回一个最合适的网络地址。<br>
	 * 运行环境没有定义本地网络地址，或者网络地址是自回路地址时使用。<br>
	 * 
	 * @return
	 */
	public static InetAddress select()  {
		InetAddress any = null;
		InetAddress link = null;
		InetAddress loopback = null;
		List<InetAddress> privates = new ArrayList<InetAddress>();
		List<InetAddress> publics = new ArrayList<InetAddress>();
		
		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			
			while (interfaces.hasMoreElements()) {
				Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress inet = addresses.nextElement();
					
					if (inet.isAnyLocalAddress()) { // 通配符地址
						any = inet;
					} else if (inet.isLoopbackAddress()) { // 自回路地址
						loopback = inet;
					} else if (inet.isLinkLocalAddress()) { // 机器自分配地址
						link = inet;
					} else if (inet.isMulticastAddress() || inet.isMCGlobal()
							|| inet.isMCLinkLocal() || inet.isMCNodeLocal()
							|| inet.isMCOrgLocal() || inet.isMCSiteLocal()) {
						// 广播地址不需要
					} else if (inet.isSiteLocalAddress()) { // 内网地址
						privates.add(inet);
					} else { // 其它地址
						publics.add(inet);
					}
				}
			}
		} catch (SocketException exp) {
			
		}
		
		// 返回顺序: 1.内网地址. 2.外网地址. 3. 机器自分配地址. 4. 自回路地址. 5. 通配符地址
		if (privates.size() > 0) return privates.get(0);
		else if (publics.size() > 0) return publics.get(0);
		else if (link != null) return link;
		else if (loopback != null) return loopback;
		else if (any != null) return any;
		
		return new Address().getAddress();
	}
	
//	/**
//	 * 串行化写入
//	 * @param writer
//	 * @throws IOException
//	 */
//	private void writeObject(ObjectOutputStream writer) throws IOException {
////		writer.defaultWriteObject();
////		byte[] b = Numeric.toBytes(this.high);
////		writer.write(b, 0, b.length);
////		b = Numeric.toBytes(this.low);
////		writer.write(b, 0, b.length);
////		b = Numeric.toBytes(this.hash);
////		writer.write(b, 0, b.length);
//		
//		System.out.println("write address object!\n");
//	}
//
//	/**
//	 * 串行化读出
//	 * @param reader
//	 * @throws IOException
//	 * @throws ClassNotFoundException
//	 */
//	private void readObject(ObjectInputStream reader) throws IOException,
//			ClassNotFoundException {
////		transient 
////		reader.defaultReadObject();
////		byte[] b = new byte[20];
////		reader.read(b, 0, b.length);
////		int seek = 0;
////		this.high = Numeric.toLong(b, seek, 8);
////		seek += 8;
////		this.low = Numeric.toLong(b, seek, 8);
////		seek += 8;
////		this.hash = Numeric.toInteger(b, seek, 4);
////		seek += 4;
//		
//		System.out.println("read address object\n");
//	}
}