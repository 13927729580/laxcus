/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * fixp security class
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/15/2009
 * 
 * @see com.lexst.fixp
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.fixp;

import java.io.*;
import java.math.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.*;
import org.w3c.dom.*;

import com.lexst.log.client.*;
import com.lexst.security.*;
import com.lexst.util.host.*;
import com.lexst.util.range.*;
import com.lexst.xml.*;

/**
 * FIXP安全通信配置类
 */
public class Security {

	/** 不检查 */
	public final static int NONE = 0;
	
	/** 地址匹配检查(集合中必须有这个地址) **/
	public final static int ADDRESS_MATCH = 1;

	/** 客户端地址范围检查 client ip address range */
	public final static int ADDRESS_CHECK = 2;

	/** 密钥检查  password check */
	public final static int CIPHERTEXT_CHECK = 3;

	/** 双重检查(即检查客户端地址范围，又检查密钥) address and encrypt */
	public final static int DOUBLE_CHECK = 4;

	/** 登录检查种类选择  **/
	private int family;
	
	/** 合法/可接受的网络地址范围集合 */
	private ArrayList<BigIntegerRange> ranges = new ArrayList<BigIntegerRange>();

	/** RSA 私钥 (服务器密钥) */
	private RSAPrivateKey rsa_prikey;
	
	/** RSA 公钥 (客户端密钥) */
	private RSAPublicKey rsa_pubkey;
	
	/**
	 * 创立安全管理类
	 */
	public Security() {
		super();
		// 默认不检查
		this.family = Security.NONE;
	}

	/**
	 * 设置安全检查种类
	 * 
	 * @param i
	 */
	public void setFamily(int i) {
		if (!(Security.NONE <= i && i <= Security.DOUBLE_CHECK)) {
			throw new IllegalArgumentException("invalid security family");
		}
		this.family = i;
	}

	/**
	 * 返回安全检查种类
	 * 
	 * @return
	 */
	public int getFamily() {
		return this.family;
	}
	
	/**
	 * 不检查
	 * @return
	 */
	public boolean isNone() {
		return this.family == Security.NONE;
	}
	
	/**
	 * 网络地址匹配检查
	 * @return
	 */
	public boolean isAddressMatch () {
		return this.family == Security.ADDRESS_MATCH;
	}
	
	/**
	 * 网络地址范围检查
	 * @return
	 */
	public boolean isAddressCheck() {
		return this.family == Security.ADDRESS_CHECK;
	}
	
	/**
	 * 密钥检查
	 * @return
	 */
	public boolean isCipherCheck() {
		return this.family == Security.CIPHERTEXT_CHECK;
	}
	
	/**
	 * 双重检查
	 * @return
	 */
	public boolean isDoubleCheck() {
		return this.family == Security.DOUBLE_CHECK;
	}

	/**
	 * 设置RSA私钥(服务器密钥)
	 * 
	 * @param key
	 */
	public void setPrivateKey(RSAPrivateKey key) {
		this.rsa_prikey = key;
	}

	/**
	 * 返回RSA私钥
	 * @return
	 */
	public RSAPrivateKey getPrivateKey() {
		return this.rsa_prikey;
	}
	
	/**
	 * 设置RSA公钥(客户端密钥)
	 * 
	 * @param key
	 */
	public void setPublicKey(RSAPublicKey key) {
		this.rsa_pubkey = key;
	}
	
	/**
	 * 返回RSA公钥
	 * @return
	 */
	public RSAPublicKey getPublicKey() {
		return this.rsa_pubkey;
	}

	public void addLegalAddress(Address begin, Address end) {
		BigIntegerRange range = new BigIntegerRange(1, begin.bits(), end.bits());
		ranges.add(range);
		Collections.sort(ranges);
	}
	
	public boolean isLegalAddress(Address address) {
		BigInteger big = new BigInteger(1, address.bits());
		for(BigIntegerRange range : ranges) {
			if( range.isInside(big)) return true;
		}
		return false;
	}
	
//	/**
//	 * @param begin
//	 * @param end
//	 */
//	public void addLegalIP(long begin, long end) {
//		LongRange range = new LongRange(begin, end);
//		ranges.add(range);
//		java.util.Collections.sort(ranges);
//	}
//
//	/**
//	 * check ip address range, when ADDRESS_CHECK
//	 * 
//	 * @param ip
//	 * @return
//	 */
//	public boolean isLegalAddress(long ip) {
//		for (LongRange range : ranges) {
//			if (range.inside(ip)) return true;
//		}
//		return false;
//	}

//	private long parseIP(String s) {
//		final String IP = "^\\s*([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\s*$";
//		Pattern pattern = Pattern.compile(IP);
//		Matcher matcher = pattern.matcher(s);
//
//		if (matcher.matches()) {
//			long value = (Long.parseLong(matcher.group(1)) & 0xFF) << 24;
//			value |= (Long.parseLong(matcher.group(2)) & 0xFF) << 16;
//			value |= (Long.parseLong(matcher.group(3)) & 0xFF) << 8;
//			value |= Long.parseLong(matcher.group(4)) & 0xFF;
//			return value;
//		}
//		return -1;
//	}
	
	private boolean parseServer(XMLocal xml, Element server) throws SecureException {
		String type = xml.getXMLValue(server.getElementsByTagName("type"));
		if ("address-match".equalsIgnoreCase(type)) {
			this.setFamily(Security.ADDRESS_MATCH);
		} else if ("address-check".equalsIgnoreCase(type)) {
			this.setFamily(Security.ADDRESS_CHECK);
		} else if ("ciphertext-check".equalsIgnoreCase(type)) {
			this.setFamily(Security.CIPHERTEXT_CHECK);
		} else if ("double-check".equalsIgnoreCase(type)) {
			this.setFamily(Security.DOUBLE_CHECK);
		} else if ("none".equalsIgnoreCase(type)) {
			this.setFamily(Security.NONE);
		} else {
			throw new IllegalArgumentException("invalid type " + type);
		}

		// legal address range
		if (this.isAddressCheck() || this.isDoubleCheck()) {
			NodeList list = server.getElementsByTagName("address-range");
			if (list != null && list.getLength() > 0) {
				for (int i = 0; i < list.getLength(); i++) {
					Element element = (Element) list.item(i);
					NodeList subs = element.getElementsByTagName("range");
					int len = subs.getLength();
					for (int j = 0; j < len; j++) {
						Element elem = (Element) subs.item(j);
						String begin = xml.getXMLValue(elem.getElementsByTagName("begin"));
						String end = xml.getXMLValue(elem.getElementsByTagName("end"));

						try {
							Address a1 = new Address(begin);
							Address a2 = new Address(end);
							this.addLegalAddress(a1, a2);
						} catch (java.net.UnknownHostException e) {
							throw new IllegalArgumentException("network address error!");
						}
						
//						long b = parseIP(begin);
//						long e = parseIP(end);
//						this.addLegalIP(b, e);
					}
				}
			}
		}

		if (this.isCipherCheck() || this.isDoubleCheck()) {
			// rsa key
			NodeList list = server.getElementsByTagName("rsa-private-key");
			Element element = (Element) list.item(0);
			list = element.getElementsByTagName("code");
						
			if (list != null && list.getLength() > 0) {
				element = (Element) list.item(0);
				String modulus = xml.getValue(element, "modulus");
				String exponent = xml.getValue(element, "exponent");
				rsa_prikey = SecureGenerator.buildRSAPrivateKey(modulus, exponent);
			} else {
				list = element.getElementsByTagName("file");
				if (list == null || list.getLength() == 0) return false;
				
				String rsafile = xml.getXMLValue(list);
				Document doc = xml.loadXMLSource(rsafile);
				if(doc == null) return false;
				list = doc.getElementsByTagName("code");
				if (list == null || list.getLength() == 0) return false;

				element = (Element) list.item(0);
				String modulus = xml.getValue(element, "modulus");
				String exponent = xml.getValue(element, "exponent");
				rsa_prikey = SecureGenerator.buildRSAPrivateKey(modulus, exponent);
			}
		}

		return true;
	}
	
	private boolean parseClient(XMLocal xml, Element client) throws SecureException {
		NodeList list = client.getElementsByTagName("rsa-public-key");
		if (list == null || list.getLength() != 1) return true;
		Element element = (Element) list.item(0);

		list = element.getElementsByTagName("code");
		if (list != null && list.getLength() > 0) {
			element = (Element) list.item(0);
			String modulus = xml.getValue(element, "modulus");
			String exponent = xml.getValue(element, "exponent");
			rsa_pubkey = SecureGenerator.buildRSAPublicKey(modulus, exponent);
		} else {
			list = element.getElementsByTagName("file");
			if (list == null || list.getLength() == 0) return false;
			
			String rsafile = xml.getXMLValue(list);
			Document doc = xml.loadXMLSource(rsafile);
			if (doc == null) return false;
			list = doc.getElementsByTagName("code");
			if (list == null || list.getLength() == 0) return false;
			
			element = (Element) list.item(0);
			String modulus = xml.getValue(element, "modulus");
			String exponent = xml.getValue(element, "exponent");
			rsa_pubkey = SecureGenerator.buildRSAPublicKey(modulus, exponent);
		}
		
		return true;
	}

	public boolean parse(String filename) {
		File file = new File(filename);
		if (!(file.exists() && file.isFile())) return false;

		XMLocal xml = new XMLocal();
		Document doc = xml.loadXMLSource(file);
		if(doc == null) return false;
		
		NodeList list = doc.getElementsByTagName("server");
		if (list == null || list.getLength() < 1) return false;
		try {
			if (!parseServer(xml, (Element) list.item(0))) return false;
		} catch (SecureException e) {
			Logger.error(e);
			return false;
		}
		
		list = doc.getElementsByTagName("client");
		if (list != null && list.getLength() == 1) {
			try {
				if (!parseClient(xml, (Element) list.item(0))) return false;
			} catch (SecureException e) {
				Logger.error(e);
				return false;
			}
		}

		return true;
	}

//	public static void main(String[] args) {
//		String filename = "D:/lexst/src/com/lexst/fixp/security.xml";
//		Security s = new Security();
//		s.parse(filename);
//		System.out.printf("RSA Key is %s\n", s.getPrivateKey()!=null ? "handle" : "NULL");
//		
//		Security ss = s;
//		System.out.printf("RSA Key is %s\n", ss.getPrivateKey()!=null ? "handle" : "NULL");
//	}

}