/**
 * 
 */
package com.lexst.sql.parse.result;

import java.util.*;

import com.lexst.util.host.*;

public class TaskHostResult {

	/** include: all, diffuse, aggregate, build */
	private String tag;
	
	/* site address list */
	private List<Address> array = new ArrayList<Address>();
	
	/**
	 * 
	 */
	public TaskHostResult() {
		super();
	}
	
	/**
	 * @param type
	 */
	public TaskHostResult(String type) {
		this();
		this.setTag(type);
	}

	public void setTag(String s) {
		this.tag = s;
	}

	public String getTag() {
		return this.tag;
	}
	
	public boolean addAddress(Address ip) {
		return array.add(ip);
	}
	
	public Address[] getAddresses() {
		Address[] s = new Address[array.size()];
		return this.array.toArray(s);
	}
	
	public boolean addAddresses(Collection<Address> all) {
		return array.addAll(all);
	}

}