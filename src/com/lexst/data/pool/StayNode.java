/**
 * 
 */
package com.lexst.data.pool;

/**
 * 存储单位
 * 
 */
final class StayNode implements Comparable<StayNode> {

	private long offset;

	private int length;

	/**
	 * default
	 */
	public StayNode(long off, int len) {
		super();
		this.setOffset(off);
		this.setLength(len);
	}

	/**
	 * 磁盘文件下标
	 * 
	 * @param i
	 */
	public void setOffset(long i) {
		this.offset = i;
	}

	public long getOffset() {
		return this.offset;
	}

	/**
	 * 数据区域长度
	 * 
	 * @param i
	 */
	public void setLength(int i) {
		this.length = i;
	}

	public int getLength() {
		return this.length;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || !(object instanceof StayNode)) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((StayNode) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (offset ^ length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(StayNode node) {
		int ret = (offset < node.offset ? -1 : (offset > node.offset ? 1 : 0));
		if (ret == 0) {
			ret = (length < node.length ? -1 : (length > node.length ? 1 : 0));
		}
		return ret;
	}

}
