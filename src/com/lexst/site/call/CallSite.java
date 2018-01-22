/**
 *
 */
package com.lexst.site.call;

import java.util.*;

import com.lexst.site.*;
import com.lexst.sql.schema.*;
import com.lexst.util.naming.*;

public class CallSite extends Site {

	private static final long serialVersionUID = -4269019030175960622L;

	/** 数据库表名 */
	private List<Space> spaces = new ArrayList<Space>();

	/** init命名集合 */
	private Set<Naming> initials = new TreeSet<Naming>();

	/** balance命名集合 */
	private Set<Naming> balances = new TreeSet<Naming>();

	/** diffuse命名集合 **/
	private Set<Naming> froms = new TreeSet<Naming>();

	/** aggregate命名集合 **/
	private Set<Naming> tos = new TreeSet<Naming>();

	/**
	 *
	 */
	public CallSite() {
		super(Site.CALL_SITE);
	}

	/**
	 * 增加一个init命名
	 * @param naming
	 * @return
	 */
	public boolean addInitial(Naming naming) {
		if (naming == null || initials.contains(naming)) {
			return false;
		}
		return this.initials.add((Naming) naming.clone());
	}

	/**
	 * 更新全部init命名
	 * @param list
	 * @return
	 */
	public int updateInitials(Collection<Naming> list) {
		initials.clear();
		for (Naming naming : list) {
			addInitial(naming);
		}
		return initials.size();
	}

	/**
	 * 返回全部init命名
	 * @return
	 */
	public Set<Naming> getInitials() {
		return this.initials;
	}

	/**
	 * 增加一个balance命名
	 * @param s
	 * @return
	 */
	public boolean addBalance(Naming s) {
		if (s == null || balances.contains(s)) {
			return false;
		}
		return balances.add((Naming) s.clone());
	}

	/**
	 * 更新全部balance命名
	 * @param list
	 * @return
	 */
	public int updateBalances(Collection<Naming> list) {
		this.balances.clear();
		for (Naming naming : list) {
			addBalance(naming);
		}
		return this.balances.size();
	}

	/**
	 * 返回全部balance命名
	 * @return
	 */
	public Set<Naming> getBalances() {
		return this.balances;
	}

	/**
	 * 增加一个diffuse命名
	 * 
	 * @param naming
	 * @return
	 */
	public boolean addFrom(Naming naming) {
		if (naming == null || froms.contains(naming)) {
			return false;
		}
		return froms.add((Naming) naming.clone());
	}

	/**
	 * 更新全部diffuse命名
	 * 
	 * @param list
	 * @return
	 */
	public int updateFroms(Collection<Naming> list) {
		froms.clear();
		for (Naming naming : list) {
			addFrom(naming);
		}
		return froms.size();
	}

	/**
	 * 增加一个aggregate命名
	 * 
	 * @param naming
	 * @return
	 */
	public boolean addTo(Naming naming) {
		if (naming == null || tos.contains(naming)) {
			return false;
		}
		return tos.add((Naming) naming.clone());
	}

	/**
	 * 更新一组aggregate命名
	 * 
	 * @param list
	 * @return
	 */
	public int updateTos(Collection<Naming> list) {
		tos.clear();
		for (Naming naming : list) {
			addTo(naming);
		}
		return tos.size();
	}

	public List<Naming> listAllNaming() {
		List<Naming> array = new ArrayList<Naming>();
		array.addAll(this.initials);
		array.addAll(this.balances);
		array.addAll(this.froms);
		array.addAll(this.tos);
		return array;
	}

	/**
	 * 增加数据库表名
	 * @param s
	 * @return
	 */
	public boolean addSpace(Space s) {
		if (s == null || this.spaces.contains(s)) {
			return false;
		}
		return this.spaces.add((Space) s.clone());
	}

	/**
	 * 删除数据库表名
	 * @param s
	 * @return
	 */
	public boolean removeSpace(Space s) {
		return this.spaces.remove(s);
	}

	/**
	 * 更新全部数据库表名
	 * @param list
	 * @return
	 */
	public int updateSpaces(Collection<Space> list) {
		this.spaces.clear();
		for (Space space : list) {
			addSpace(space);
		}
		return this.spaces.size();
	}

	/**
	 * 返回全部数据库表名
	 * @return
	 */
	public Collection<Space> getSpaces() {
		return this.spaces;
	}

	/**
	 * 统计数据库表名数量
	 * @return
	 */
	public int countSpaces() {
		return this.spaces.size();
	}

}