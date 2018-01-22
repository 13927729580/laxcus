/**
 * 
 */
package com.lexst.debug;

/**
 * @author siven
 *
 */
public class Block {
	
	private long key;
	private int weight;
	private byte type;
	private byte site;
	private short showTime;
	private int publishTime;

	public Block() {
		super();
		weight = 0;
	}
	
	public Block(long key) {
		this();
		this.setKey(key);
	}
	
	public void setKey(long i) {
		key = i;
	}
	public long getKey() {
		return key;
	}
	
	public void addWeight(int value) {
		this.weight += value;
	}
	public int getWeight() {
		return this.weight;
	}
	
	public void setType(byte b) {
		type = b;
	}
	public byte getType() {
		return type ;
	}
	
	public void setSite(byte b) {
		site = b;
	}
	public byte getSite() {
		return site;
	}
	
	public void setShowTime(short time) {
		this.showTime = time;
	}
	public int getShowTime() {
		return this.showTime;
	}
	
	public void setPublishTime(int time) {
		this.publishTime = time;
	}
	public int getPublishTime() {
		return this.publishTime;
	}
	
	public int build(int i) throws java.net.UnknownHostException  {
		if(i != 0) {
			throw new java.net.UnknownHostException("this is error!");
		}
		return i+1;
	}
	
	public int test(int i) {
		try {
			return build(i);
		} catch (java.net.UnknownHostException e) {
//			assert true;
//			e.printStackTrace();
		}
		return -1;
	}
	
	public static void main(String[] args) {
		Block b = new Block();
		b.test(12);
	}
}
