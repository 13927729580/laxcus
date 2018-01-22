/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.parse.result;

import java.util.*;

import com.lexst.util.host.*;

/**
 * @author scott.liang
 *
 */
public class ShowSiteResult {
	
	private int siteFimaly;
	
	private List<Address> array = new ArrayList<Address>();

	/**
	 * 
	 */
	public ShowSiteResult(int fimaly) {
		super();
		this.siteFimaly = fimaly;
	}
	
	public int getSiteFimaly() {
		return this.siteFimaly;
	}
	
	public boolean addAddress(Address ip) {
		if (!array.contains(ip)) {
			return array.add(ip);
		}
		return false;
	}

	/**
	 * 返回地址集合
	 * @return
	 */
	public Address[] getAddresses() {
		if(array.isEmpty()) {
			return null;
		}
		Address[] s = new Address[array.size()];
		return this.array.toArray(s);
	}

}
