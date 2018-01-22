/**
 * DATA主节点主块重构时间触发器
 */
package com.lexst.sql.schema;

import java.io.*;
import java.util.*;

import com.lexst.util.datetime.*;

/**
 * 当发生删除或者更新时，磁盘上会产生数据碎片和冗余数据，<br>
 * DATA节点为提高检索效率，需要定时清理它们，重构数据块。<br>
 * 此类是触发时间定义。<br>
 * <br>
 * 监控在TOP节点进行，到达指定时间后，通知HOME节点，再由HOME节点通知各DATA主节点重构数据。<br>
 * DATA节点操作过程：删除磁盘上的过期记录，重新排列有效记录，达到节省空间和提高检索效率的目标的<br>
 *
 */
public class SwitchTime implements Serializable, Cloneable {
	
	private static final long serialVersionUID = 1L;

	/** 触发时间定义：每小时、每天、每周、每月 **/
	public final static int HOURLY = 1;
	public final static int DAILY = 2;
	public final static int WEEKLY = 3;
	public final static int	MONTHLY = 4;

	/** 数据表名称 **/
	private Space space;

	/** 重构列ID，默认是0，即主键(prime key)为索引 */
	private short columnId;

	/** 触发类型，见解发时间定义 */
	private int type;

	/** 触发间隔 */
	private long interval;

	/** 触发时间(毫秒) */
	private long endTime;

	/**
	 * default
	 */
	public SwitchTime() {
		super();
		this.columnId = 0;
		this.type = 0;
		this.interval = 0L;
		this.endTime = 0L;
	}
	
	/**
	 * @param space
	 */
	public SwitchTime(Space space) {
		this();
		this.setSpace(space);
	}
	
	/**
	 * 复制SwitchTime参数
	 * 
	 * @param object
	 */
	public SwitchTime(SwitchTime object) {
		this();
		this.setSpace(object.space);
		this.setColumnId(object.columnId);
		this.setType(object.type);
		this.setInterval(object.interval);
		this.endTime = object.endTime;
	}

	/**
	 * 设置表名
	 * @param s
	 */
	public void setSpace(Space s) {
		this.space = new Space(s);
	}

	/**
	 * 取表名
	 * @return
	 */
	public Space getSpace() {
		return this.space;
	}
	
	/**
	 * 设置重构索引键
	 * @param id
	 */
	public void setColumnId(short id) {
		this.columnId = id;
	}
	
	/**
	 * 返回重构索引键
	 * @return
	 */
	public short getColumnId() {
		return this.columnId;
	}

	/**
	 * 定义触发类型
	 * 
	 * @param i
	 */
	public void setType(int i) {
		if (!(SwitchTime.HOURLY <= i && i <= SwitchTime.MONTHLY)) {
			throw new IllegalArgumentException("invalid type");
		}
		this.type = i;
	}

	/**
	 * 取触发类型
	 * 
	 * @return
	 */
	public int getType() {
		return this.type;
	}

	/**
	 * 设置触发时间间隔
	 * 
	 * @param i
	 */
	public void setInterval(long i) {
		this.interval = i;
	}

	/**
	 * 返回触发时间间隔
	 * @return
	 */
	public long getInterval() {
		return this.interval;
	}
	
	/**
	 * 计算触发时间
	 * 
	 * set rebuild time schema.table hourly "12:12" order by column-name
	 * set rebuild time schema.table daily "0:23:12"		
	 * set rebuild time schema.table weekly "1 0:12:12"  	(1-7)
	 * set rebuild time schema.table monthly "31 0:12:23"	(1-31)
	 */
	public void nextTouch() {
		Calendar end = Calendar.getInstance();
		end.set(Calendar.MILLISECOND, 0);
		Calendar date = Calendar.getInstance();
		date.setTime(SimpleTimestamp.format(interval));
		
		switch (type) {
		case SwitchTime.HOURLY: // 每时触发时间
			end.set(Calendar.MINUTE, date.get(Calendar.MINUTE));
			end.set(Calendar.SECOND, date.get(Calendar.SECOND));
			System.out.println(end.getTime().toString());
			System.out.printf("%d - %d\n", System.currentTimeMillis(), end.getTimeInMillis());
			// 如果当前时间超过指定时间，移到下一次发生的时间
			
			if (System.currentTimeMillis() > end.getTimeInMillis()) {
				end.add(Calendar.HOUR, 1);
			}
			System.out.printf("%d - %d\n", System.currentTimeMillis(), end.getTimeInMillis());
			endTime = end.getTimeInMillis();
			break;
		case SwitchTime.DAILY: // 每天触发时间
			end.set(Calendar.HOUR_OF_DAY, date.get(Calendar.HOUR_OF_DAY));
			end.set(Calendar.MINUTE, date.get(Calendar.MINUTE));
			end.set(Calendar.SECOND, date.get(Calendar.SECOND));
			if (System.currentTimeMillis() > end.getTimeInMillis()) {
				end.add(Calendar.DAY_OF_MONTH, 1);
			}
			endTime = end.getTimeInMillis();
			break;
		case SwitchTime.WEEKLY: // 每周触发时间
			end.set(Calendar.DAY_OF_WEEK, date.get(Calendar.DAY_OF_MONTH));
			end.set(Calendar.HOUR_OF_DAY, date.get(Calendar.HOUR_OF_DAY));
			end.set(Calendar.MINUTE, date.get(Calendar.MINUTE));
			end.set(Calendar.SECOND, date.get(Calendar.SECOND));
			if (System.currentTimeMillis() > end.getTimeInMillis()) {
				end.add(Calendar.WEEK_OF_MONTH, 1);
			}
			endTime = end.getTimeInMillis();
			break;
		case SwitchTime.MONTHLY: // 每月触发时间
			end.set(Calendar.DAY_OF_MONTH, date.get(Calendar.DAY_OF_MONTH));
			end.set(Calendar.HOUR_OF_DAY, date.get(Calendar.HOUR_OF_DAY));
			end.set(Calendar.MINUTE, date.get(Calendar.MINUTE));
			end.set(Calendar.SECOND, date.get(Calendar.SECOND));
			if (System.currentTimeMillis() > end.getTimeInMillis()) {
				end.add(Calendar.MONTH, 1);
			}
			endTime = end.getTimeInMillis();
			break;
		}
	}

	/**
	 * 是否到达触发时间
	 * @return
	 */
	public boolean isTouched() {
		if (endTime == 0L) {
			this.nextTouch();
		}
		return System.currentTimeMillis() >= endTime;
	}

	/*
	 * 复制SwitchTime对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new SwitchTime(this);
	}

}