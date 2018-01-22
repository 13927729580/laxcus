/**
 *
 */
package com.lexst.site.home;

import java.net.*;
import java.util.*;

import com.lexst.site.*;
import com.lexst.sql.account.*;
import com.lexst.sql.schema.*;

public class HomeSite extends Site {

	private static final long serialVersionUID = 1L;
	
	private transient boolean publish;
	
	/* home user account */
	private User user;

	/* table space set */
	private List<Space> array = new ArrayList<Space>();
	
	private boolean runflag;

	/**
	 *
	 */
	public HomeSite() {
		super(Site.HOME_SITE);
		this.runflag = false;
	}

	/**
	 * @param address
	 * @param tcport
	 * @param udport
	 */
	public HomeSite(InetAddress address, int tcport, int udport) {
		this();
		super.setHost(address, tcport, udport);
	}

	/**
	 * @param address - IP地址或者主机域名格式
	 * @param tcport
	 * @param udport
	 */
	public HomeSite(String address, int tcport, int udport) throws UnknownHostException {
		this(InetAddress.getByName(address), tcport, udport);
	}
	
//	/**
//	 * @param ip
//	 * @param tcport
//	 * @param udport
//	 */
//	public HomeSite(int ip, int tcport, int udport) {
//		this();
//		super.setHost(ip, tcport, udport);
//	}
	
	public void setRunsite(boolean b) {
		this.runflag = b;
	}
	public boolean isRunsite() {
		return this.runflag;
	}
	
	public void setPublish(boolean b) {
		this.publish = b;
	}
	public boolean isPublish() {
		return this.publish;
	}
	
	public void setUser(String username, String password) {
		this.user = new User(username, password);
//		this.user.setTextPassword(password);
	}
	
	public User getUser() {
		return this.user;
	}

	public boolean addSpace(Space space) {
		if(array.contains(space)) {
			return false;
		}
		return array.add(space);
	}

	public boolean removeSpace(Space space) {
		if (!array.contains(space)) {
			return false;
		}
		return array.remove(space);
	}

	public List<Space> listSpace() {
		return array;
	}

}