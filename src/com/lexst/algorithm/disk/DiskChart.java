/**
 * @email admin@wigres.com
 *
 */
package com.lexst.algorithm.disk;

import java.util.*;

import com.lexst.sql.conduct.matrix.*;

/**
 * @author scott.liang
 *
 */
final class DiskChart implements Comparable<DiskChart>{
	
	/** 任务号 */
	private long jobid;

	/** 数据分片信息 */
	private ArrayList<DiskField> array = new ArrayList<DiskField>(5);

	/**
	 * default
	 */
	public DiskChart() {
		super();
	}

	/**
	 * 构造函数，指定任务号
	 * @param jobid
	 */
	public DiskChart(long jobid) {
		this();
		this.setJobid(jobid);
	}

	/**
	 * 设置任务号
	 * 
	 * @param i
	 */
	public void setJobid(long i) {
		this.jobid = i;
	}

	/**
	 * 返回任务号
	 * 
	 * @return
	 */
	public long getJobid() {
		return this.jobid;
	}

	public boolean add(DiskField field) {
		if (array.contains(field))
			return true;
		return array.add(field);
	}

	public boolean remove(DiskField filed) {
		return array.remove(filed);
	}

	public boolean isEmpty() {
		return array.isEmpty();
	}

	public int size() {
		return array.size();
	}
	
	public void trim() {
		array.trimToSize();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object arg) {
		if (arg == null || !(arg instanceof DiskChart)) {
			return false;
		} else if (arg == this) {
			return true;
		}

		DiskChart a = (DiskChart) arg;
		return jobid == a.jobid;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (jobid >>> 32 & jobid);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DiskChart chart) {
		return (jobid < chart.jobid ? -1 : (jobid > chart.jobid ? 1 : 0));
	}
}