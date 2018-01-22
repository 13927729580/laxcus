package com.lexst.sql.schema;

import java.io.*;

import com.lexst.util.naming.*;

/**
 * 测点：由命名对象和数据库表名组成。<br>
 * 标记一台主机上的表和命名的配置对象。<br>
 * 
 * 
 */
public final class Anchor implements Serializable, Cloneable, Comparable<Anchor> {

	private static final long serialVersionUID = -2460056501058811503L;

	/** 任务命名 */
	private Naming naming;

	/** 数据库表名 */
	private Space space;

	/**
	 * default
	 */
	public Anchor() {
		super();
	}

	/**
	 * 根据任务命名和数据库表构造实例
	 * 
	 * @param naming
	 * @param space
	 */
	public Anchor(Naming naming, Space space) {
		this();
		this.set(naming, space);
	}

	/**
	 * 复制对象
	 * 
	 * @param anchor
	 */
	public Anchor(Anchor anchor) {
		this();
		this.set(anchor.naming, anchor.space);
	}

	/**
	 * 设置参数
	 * 
	 * @param naming
	 * @param space
	 */
	public void set(Naming naming, Space space) {
		this.naming = new Naming(naming);
		this.space = new Space(space);
	}

	/**
	 * 返回任务命名
	 * 
	 * @return
	 */
	public Naming getNaming() {
		return this.naming;
	}

	/**
	 * 返回数据库表名
	 * 
	 * @return
	 */
	public Space getSpace() {
		return this.space;
	}

	/**
	 * @param anchor
	 * @return
	 */
	@Override
	public int compareTo(Anchor anchor) {
		if (anchor == null) {
			return 1;
		}
		int ret = naming.compareTo(anchor.naming);
		if (ret == 0) {
			ret = space.compareTo(anchor.space);
		}
		return ret;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != Anchor.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((Anchor) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return naming.hashCode() ^ space.hashCode();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Anchor(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s:%s", naming.toString(), space.toString());
	}


}
