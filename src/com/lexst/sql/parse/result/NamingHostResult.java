/**
 * 
 */
package com.lexst.sql.parse.result;

import java.util.*;

import com.lexst.util.host.*;

/**
 * 命名主机
 *
 */
public class NamingHostResult {
	
	/** 任务命名(必需) */
	private String naming;
	
	/** 主机地址集合 */
	private ArrayList<Address> array = new ArrayList<Address>(3);

	/**
	 * 
	 */
	public NamingHostResult() {
		super();
	}
	
	/**
	 * @param naming
	 */
	public NamingHostResult(String naming) {
		this();
		this.setNaming(naming);
	}

	public void setNaming(String s) {
		this.naming = s;
	}

	public String getNaming() {
		return this.naming;
	}
	
	public boolean addAddress(Address ip) {
		return array.add(ip);
	}
	
	public Address[] getAddresses() {
		Address[] s = new Address[array.size()];
		return array.toArray(s);
	}
	
	public boolean addAddresses(Collection<Address> s) {
		return array.addAll(s);
	}

}