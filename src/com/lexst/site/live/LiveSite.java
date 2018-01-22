/**
 * 
 */
package com.lexst.site.live;

import java.util.*;

import com.lexst.site.*;
import com.lexst.sql.account.*;
import com.lexst.sql.schema.*;

public class LiveSite extends Site {
	private static final long serialVersionUID = 1L;
	
	/** 用户账号 **/
	private User user;
	
	/** 账号下的可操作的数据库表集合  **/
	private List<Space> array = new ArrayList<Space>();
	
	/** 通信安全算法 algorithm name, eg: aes, des, des3 */
	private String algo;
	
	/**
	 * 
	 */
	public LiveSite() {
		super(Site.LIVE_SITE);
	}
	
	/**
	 * @param site
	 */
	public LiveSite(LiveSite site) {
		super(site);
		array = new ArrayList<Space>(site.array);
	}

	/**
	 * 设置注册用户账号
	 * @param username
	 * @param password
	 */
	public void setUser(String username, String password) {
		this.user = new User(username, password);
	}

	/**
	 * 设置注册用户账号
	 * @param account
	 */
	public void setUser(User account) {
		this.user = new User(account);
	}

	/**
	 * 返回注册用户账号
	 * @return
	 */
	public User getUser() {
		return this.user;
	}
	
	/**
	 * 设置安全通信算法
	 * @param s
	 */
	public void setAlgorithm(String s) {
		this.algo = s;
	}
	/**
	 * get algorithm name
	 * @return
	 */
	public String getAlgorithm() {
		return this.algo;
	}

	public boolean add(Space space) {
		if(array.contains(space)) {
			return false;
		}
		return array.add(space);
	}

	public boolean remove(Space space) {
		return array.remove(space);
	}

	public boolean contains(Space space) {
		return array.contains(space);
	}

	public Collection<Space> list() {
		return array;
	}
	
	public void clear() {
		array.clear();
	}

	public boolean isEmpty() {
		return array.isEmpty();
	}

	public int size() {
		return array.size();
	}
}
