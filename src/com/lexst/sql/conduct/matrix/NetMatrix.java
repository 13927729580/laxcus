/**
 * 
 */
package com.lexst.sql.conduct.matrix;

import java.io.*;
import java.util.*;

import com.lexst.util.host.*;

/**
 * 计算完成后(diffuse or aggregate)，形成以模(mod)为键值，"模->分布图谱"的集合信息。<br>
 * <br>
 * 过程: <br>
 * 1. call节点收集被调用的data、work节点返回的DiskArea信息<br>
 * 2. 将DiskArea中的信息拆解为DiskField<br>
 * 3. 以DiskField中的模为键，Domain为值，形成这个信息集合<br>
 */
public class NetMatrix implements Serializable, Cloneable {
	
	private static final long serialVersionUID = -1547026948320911027L;
	
	// spread, view, NetView
	
	/** 模(mod，唯一性) -> 分布图谱(多个节点同一模值的信息)  **/ 
	private Map<Integer, NetDomain> domains = new TreeMap<Integer, NetDomain>();
	
	/**
	 * default
	 */
	public NetMatrix() {
		super();
	}

	/**
	 * @param area
	 */
	public NetMatrix(DiskArea area) {
		this();
		this.add(area);
	}

	/**
	 * @param list
	 */
	public NetMatrix(List<DiskArea> list) {
		this();
		this.add(list);
	}

	/**
	 * 拆解Area将Field按模值保存，返回保存的Field数量
	 * 
	 * @param area
	 * @return
	 */
	public int add(DiskArea area) {
		int count = 0;
		for (DiskField field : area.list()) {
			int mod = field.getMod();
			NetDomain domain = domains.get(mod);
			if (domain == null) {
				domain = new NetDomain();
				domains.put(mod, domain);
			}
			long jobid = area.getJobid();
			SiteHost host = area.getHost();
			int timeout = area.getTimeout();
			boolean b = domain.add(jobid, host, timeout, field);
			if (b) count++;
		}
		return count;
	}
	
	/**
	 * 拆解一组DiskArea，将Field按模值保存，返回保存的Field数量
	 * @param list
	 * @return
	 */
	public int add(List<DiskArea> list) {
		int count = 0;
		for (DiskArea area : list) {
			count += add(area);
		}
		return count;
	}

	/**
	 * 返回模值集合
	 * @return
	 */
	public Set<Integer> keySet() {
		return domains.keySet();
	}

	/**
	 * 查找分区数据段
	 * @param mod
	 * @return
	 */
	public NetDomain get(int mod) {
		return domains.get(mod);
	}

	/**
	 * 检测数组是否空
	 * @return
	 */
	public boolean isEmpty() {
		return domains.isEmpty();
	}

	/**
	 * 返回数组尺寸
	 * @return
	 */
	public int size() {
		return domains.size();
	}

	/**
	 * 统计各存储节点(data、work)上的数据总长度
	 * 
	 * @return
	 */
	public long length() {
		long len = 0L;
		for (NetDomain value : domains.values()) {
			len += value.length();
		}
		return len;
	}
	
	/**
	 * 根据节点数和模值为依据进行分割/合并<br>
	 * 算法：<br>
	 * <1> 如果节点数目大于模值总量，返回模值总量的数据片数组<br>
	 * <2> 如果节点数目小于模值总量，返回节点数量的数据片数组，这时的数量片以相邻模值进行了合并。<br><br>
	 * 
	 * 不会发生分配后的结果，数据片数组大于节点数目的可能。即 sites>domains[].size 不会出现<br>
	 * 模值(mod)的隐性含义: 相邻的模值，它们对应的实际数据也是相邻的。这一点是分片的基本规则。<br><br>
	 * 
	 * 自定义的balance算法也要遵循此规定。<br>
	 * 
	 * @param sites
	 * @return
	 */
	public NetDomain[] balance(final int sites) {
		if (sites < 1) {
			throw new IllegalArgumentException("invalid sites:" + sites);
		}

		// 如果网络节点数大于分片数，以分片数量为准
		if (sites >= domains.size()) {
			NetDomain[] s = new NetDomain[domains.size()];
			int index = 0;
			for (NetDomain n : domains.values()) {
				s[index++] = (NetDomain) n.clone();
			}
			return s;
		}
		
		// 否则，将以节点数量为准进行收缩
		ArrayList<Integer> a = new ArrayList<Integer>(sites);
		int count = 0;
		// 收缩后，每一组分配的数量。值保存到数组
		int scale = domains.size() / sites;
		if(domains.size() % sites != 0) scale++;
		
		while (a.size() < sites) {
			// 每组默认分配的数量
			int number = scale;
			// 预分配后，还剩下的数量
			int left = domains.size() - (count + number);
//			System.out.printf("%d , %d , %d\n", left, sites, a.size());

			// 剩余量不足填满时，值减1。"sites - (a.size() + 1)"， 是添填后的剩余量
			if (left < sites - (a.size() + 1)) {
				number--;
			}

			a.add(number);
			count += number;
		}
		
		// debug code, start
		for(int number : a) {
			System.out.printf("number:%d\n", number);
		}
		// debug code, end
		
		// 取相邻的点合并
		NetDomain[] s = new NetDomain[a.size()];
		ArrayList<Integer> mods = new ArrayList<Integer>(domains.keySet());
		int index = 0;
		for (int number : a) {
			// 每次取几个模值
			for (int i = 0; i < number; i++) {
				int mod = mods.remove(0);
				NetDomain n = domains.get(mod);
				if (s[index] == null) {
					s[index] = (NetDomain) n.clone();
				} else {
					s[index].add(n);
				}
			}
			index++;
		}
		
		return s;
	}
	

//	/**
//	 * 根据网络节点数和数据总长度， 平均分配每段数据区域，实现数据平衡
//	 * 
//	 * 分割条件:
//	 * <1> 以模为基础进行分割，节点数大于等于模值，以模值为准；否则以节点数为准
//	 * 
//	 * @param sites
//	 * @return
//	 */
//	public NetDomain[] balance2(int sites) {
//		if (sites < 1) {
//			throw new IllegalArgumentException("invalid sites:" + sites);
//		}
//
//		// 字节流的总长度
//		long total = length();
//		// 平衡每个节点应该分配的字节流长度
//		long scale = total / sites;
//		if (total % sites != 0) scale++;
//		
//		final int def = (sites >= domains.size() ? domains.size() : sites);
//		
//		// 模值  -> 数据流长度
//		Map<Integer, Long> mapSizes = new TreeMap<Integer, Long>();
//		for (int mod : domains.keySet()) {
//			NetDomain table = domains.get(mod);
//			mapSizes.put(mod, table.length());
//		}
//		
//		// 以数据总长度为基础，保持分布平衡
//		
//		ArrayList<ISet> sets = new ArrayList<ISet>();
//		ArrayList<Integer> store = new ArrayList<Integer>();
//		ArrayList<Integer> array = new ArrayList<Integer>(mapSizes.keySet());
//		Map<Long, ISet> mapSet = new TreeMap<Long, ISet>();
//		
//		for(int i = 0; i < array.size(); i++) {
//			int mod1 = array.get(i);
//			if(store.contains(mod1)) continue;
//			
//			mapSet.clear();
//			long length1 = mapSizes.get(mod1);
//			
//			if (length1 == scale) {
//				mapSet.put(length1, new ISet(mod1));
//			} else {
//				for (int j = 0; j < array.size(); j++) {
//					if (i == j) continue;
//					int mod2 = array.get(j);
//					if (store.contains(mod2)) continue;
//					long length2 = mapSizes.get(mod2);
//
//					// 如果其中之一匹配就保存
//					if (length2 == scale) {
//						mapSet.clear();
//						mapSet.put(length2, new ISet(mod2));
//						i--;
//						break;
//					}
//
//					long len = length1 + length2;
//					if (len == scale) { // 最佳匹配,其它全部清空,保存退出
//						mapSet.clear();
//						mapSet.put(len, new ISet(mod1, mod2));
//						break;
//					}
//
//					if (mapSet.isEmpty()) { // 空状态,保存
//						mapSet.put(len, new ISet(mod1, mod2));
//					} else {
//						Map<Long, ISet> map3 = new TreeMap<Long, ISet>();
//						for (long length : mapSet.keySet()) {
//							// 在原基础上增加
//							long newlen = length + length2;
//							if (newlen < 1) continue; // 数溢出,不处理
//
//							ISet set2 = mapSet.get(length);
//							map3.put(newlen, new ISet(set2, mod2));
//						}
//						mapSet.putAll(map3);
//					}
//				}
//			}
//
//			if(mapSet.isEmpty()) {
//				// 唯一,保留它
//				store.add(mod1);
//				sets.add(new ISet(mod1));
//			} else {
//				// 找到大于等于平均值,但是又是最小的
//				long length = 0;
//				for(long len : mapSet.keySet()) {
////					if (length == 0 || length > len) length = len;
//					
//					if(length == 0) length = len;
//					else if (len >= scale && (length < scale || length > len)) length = len;
//				}
//				ISet set = mapSet.get(length);
//				store.addAll(set.set());
//				sets.add(set);
//			}
//		}
//
//		NetDomain[] tables = new NetDomain[sets.size()];
//		int index = 0;
//		for (ISet set : sets) {
//			tables[index] = new NetDomain();
//			for (int mod : set.set()) {
//				NetDomain table = domains.get(mod);
//				tables[index].add(table);
//			}
//			index++;
//		}
//		return tables;
//	}

}