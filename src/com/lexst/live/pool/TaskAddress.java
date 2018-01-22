/**
 * 
 */
package com.lexst.live.pool;


public class TaskAddress implements Comparable<TaskAddress> {

	private String naming;
	private String address;
	
	/**
	 * 
	 */
	public TaskAddress(String naming, String address) {
		super();
		this.naming = naming;
		this.address = address;
	}

	public String getNaming() {
		return this.naming;
	}
	
	public String getAddress() {
		return this.address;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(TaskAddress arg) {
		return naming.compareToIgnoreCase(arg.naming);
	}
}