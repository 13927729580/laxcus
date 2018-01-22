/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * column index set
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 12/7/2009
 * 
 * @see com.lexst.sql.index.chart
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.index.chart;

import java.util.*;

import com.lexst.sql.index.balance.*;
import com.lexst.sql.index.range.*;
import com.lexst.sql.statement.*;
import com.lexst.util.host.*;

/**
 * HOME集群下，一个表在全部data节点上的某列索引值映象集合
 */
public interface IndexChart {

	/**
	 * 记录一台data主机下，某个表某个块(chunk)下的索引记录
	 * @param host
	 * @param index
	 * @return
	 */
	boolean add(SiteHost host, IndexRange index);

	/**
	 * 删除一台data主机下的某个数据块记录
	 * @param host
	 * @param chunkid
	 * @return
	 */
	int remove(SiteHost host, long chunkid);

	/**
	 * 删除一台主机下的索引记录
	 * @param host
	 * @return
	 */
	int remove(SiteHost host);

	/**
	 * 删除基于某个主机下的索引集合，并且返回这台主机上的chunkid
	 * @param host
	 * @return
	 */
	List<Long> delete(SiteHost host);

	/**
	 * 根据条件，检索匹配的chunkid
	 * @param condi
	 * @return
	 */
	Set<Long> find(Condition condi);
	
//	/**
//	 * 返回这一列的范围集合，如果ignore is true,后面两个参数必须存在
//	 * @param ignore
//	 * @param ignoreBegin
//	 * @param ignoreEnd
//	 * @return
//	 */
//	IndexZone[] find(boolean ignore, java.lang.Number ignoreBegin, java.lang.Number ignoreEnd);

	/**
	 * 筛选符合条件的索引区域
	 * 
	 * @param ignore - is true，即需要筛选。此时 b 和 e不可以null 。
	 * @param b - 过滤索引区的开始下标集合
	 * @param e - 过滤索引区的截止下标集合
	 * @return
	 */
	IndexZone[] choice(boolean ignore, java.lang.Number[] b, java.lang.Number[] e);
	
	/**
	 * 筛选符合条件的索引区域
	 * 
	 * @param ignore
	 * @return
	 */
	IndexZone[] choice(boolean ignore);
	
	/**
	 * 返回这一列的全部chunkid
	 * @return
	 */
	Set<Long> getChunkIds();

	/**
	 * 集合是否为空
	 * @return
	 */
	boolean isEmpty();

	/**
	 * 集合成员数目
	 * @return
	 */
	int size();
}