/**
 *
 */
package com.lexst.sql.account;

import java.io.Serializable;
import java.util.*;

/**
 * 权限选项集<br>
 * 包括针对数据库表、数据库、注册用户三种类型
 *
 */
public class Control implements Serializable, Cloneable {

	private static final long serialVersionUID = 4543903501914607174L;

	/** 数据库表授权权限选项 **/
	public final static int SELECT = 1;
	public final static int INSERT = 2;
	public final static int DELETE = 3;
	public final static int UPDATE = 4;
	public final static int CONDUCT = 5;

	/** 数据库权限选项 **/
	public final static int CREATE_TABLE = 10;
	public final static int DROP_TABLE = 11;

	/** 注册用户权限选项 **/
	public final static int GRANT = 20;
	public final static int REVOKE = 21;

	public final static int CREATE_SCHEMA = 22;
	public final static int DROP_SCHEMA = 23;

	public final static int CREATE_USER = 24;
	public final static int DROP_USER = 25;
	public final static int ALTER_USER = 26;

	public final static int DBA = 27;

	/** 允许全部选项 **/
	public final static int ALL = 30;

	/** 数据库管理员账号选项(允许做任何操作) */
	private final static int[] DBA_OPTIONS = { Control.DBA,
			Control.CREATE_USER, Control.DROP_USER, Control.ALTER_USER,
			Control.GRANT, Control.REVOKE, Control.CREATE_SCHEMA,
			Control.DROP_SCHEMA, Control.CREATE_TABLE, Control.DROP_TABLE,
			Control.SELECT, Control.DELETE, Control.INSERT, Control.UPDATE,
			Control.CONDUCT };

	/** 数据库表选项 */
	private final static int[] TABLE_OPTIONS = new int[] { Control.SELECT,
			Control.DELETE, Control.INSERT, Control.UPDATE, Control.CONDUCT };

	/** 数据库选项 **/
	private final static int[] SCHEMA_OPTIONS = new int[] {
			Control.CREATE_TABLE, Control.DROP_TABLE };

	/** 注册用户账号选项 **/
	private final static int[] USER_OPTIONS = { Control.CREATE_USER,
			Control.DROP_USER, Control.ALTER_USER, Control.GRANT,
			Control.REVOKE, Control.CREATE_SCHEMA, Control.DROP_SCHEMA,
			Control.CREATE_TABLE, Control.DROP_TABLE, Control.INSERT,
			Control.DELETE, Control.UPDATE, Control.SELECT, Control.CONDUCT };
	
	/** 选项标识号集合 **/
	private Set<Integer> set = new TreeSet<Integer>();

	/**
	 * 初始化对象
	 */
	public Control() {
		super();
	}

	/**
	 * 复制对象
	 * @param object
	 */
	public Control(Control object) {
		this();
		this.set.addAll(object.set);
	}

	/**
	 * 设置一组权限
	 * @param actives
	 * @return
	 */
	private int set(int[] actives) {
		int size = set.size();
		for (int i = 0; actives != null && i < actives.length; i++) {
			this.add(actives[i]);
		}
		return set.size() - size;
	}
	
	/**
	 * 是否允许的选项
	 * @param id
	 * @return
	 */
	public boolean isAllow(int id) {
		return set.contains(new Integer(id));
	}
	
	/**
	 * 检查/设置表权限
	 * @param actives
	 * @return
	 */
	private boolean setTable(int[] actives) {
		int index = 0;
		boolean all = false;
		for (int active : actives) {
			for (index = 0; index < TABLE_OPTIONS.length; index++) {
				if (TABLE_OPTIONS[index] == active) break;
			}
			if (!all && active == Control.ALL) {
				all = true;
			} else if (index == TABLE_OPTIONS.length) {
				return false;
			}
		}
		if (all) {
			this.set(TABLE_OPTIONS);
		} else {
			this.set(actives);
		}
		return true;
	}

	/**
	 * 检查/设置数据库权限
	 * @param actives
	 * @return
	 */
	private boolean setSchema(int[] actives) {
		int index = 0;
		boolean all = false;
		for(int active : actives) {
			for (index = 0; index < SCHEMA_OPTIONS.length; index++) {
				if (SCHEMA_OPTIONS[index] == active) break;
			}
			if(!all && active == Control.ALL) {
				all = true;
			} else if(index == SCHEMA_OPTIONS.length) {
				return false;
			}
		}
		if(all) {
			set(SCHEMA_OPTIONS);
		} else {
			set(actives);
		}
		return true;
	}
	
	/**
	 * 检查/设置注册用户权限
	 * @param actives
	 * @return
	 */
	private boolean setUser(int[] actives) {
		int index = 0;
		boolean dba = false, all = false;
		for (int active : actives) {
			for (index = 0; index < USER_OPTIONS.length; index++) {
				if (USER_OPTIONS[index] == active) break;
			}
			if (!all && active == Control.ALL) {
				all = true;
			} else if (!dba && active == Control.DBA) {
				dba = true;
			} else if (index == USER_OPTIONS.length) {
				return false;
			}
		}
		if (dba) {
			set(Control.DBA_OPTIONS);
		} else if (all) {
			set(Control.USER_OPTIONS);
		} else {
			set(actives);
		}
		return true;
	}

	/**
	 * 根据级别设置控制选项
	 * @param level
	 * @param actives
	 * @return
	 */
	public boolean set(int level, int[] actives) {
		if (level == Permit.TABLE_PERMIT) {
			return this.setTable(actives);
		} else if (level == Permit.SCHEMA_PERMIT) {
			return this.setSchema(actives);
		} else if (level == Permit.USER_PERMIT) {
			return this.setUser(actives);
		}
		return false;
	}
	
//	/**
//	 * 根据级别设置控制选项
//	 * @param level
//	 * @param actives
//	 * @return
//	 */
//	public boolean set(int level, int[] actives) {
//		if (level == Permit.TABLE_PERMIT) {
//			return this.setTable(actives);
//		} else if(level == Permit.SCHEMA_PERMIT ) {
//			return this.setSchema(actives);
//		} else if(level == Permit.USER_PERMIT) {
//			return this.setUser(actives);
//		}
//		return false;
//		
//		
//		int index = 0;
//
//		if (level == Permit.TABLE_PERMIT) {
//			// check actives
//			boolean all = false;
//			for (int active : actives) {
//				for (index = 0; index < TABLE_OPTIONS.length; index++) {
//					if (TABLE_OPTIONS[index] == active) break;
//				}
//				if (!all && active == Control.ALL) {
//					all = true;
//				} else if (index == TABLE_OPTIONS.length) {
//					return false;
//				}
//			}
//			if (all) {
//				this.set(TABLE_OPTIONS);
//			} else {
//				this.set(actives);
//			}
//			return true;
//		} else if(level == Permit.SCHEMA_PERMIT ) {
//			boolean all = false;
//			for(int active : actives) {
//				for (index = 0; index < SCHEMA_OPTIONS.length; index++) {
//					if (SCHEMA_OPTIONS[index] == active) break;
//				}
//				if(!all && active == Control.ALL) {
//					all = true;
//				} else if(index == SCHEMA_OPTIONS.length) {
//					return false;
//				}
//			}
//			if(all) {
//				set(SCHEMA_OPTIONS);
//			} else {
//				set(actives);
//			}
//			return true;
//		} else if(level == Permit.USER_PERMIT) {
//			boolean dba = false, all = false;
//			for(int active : actives) {
//				for (index = 0; index < USER_OPTIONS.length; index++) {
//					if(USER_OPTIONS[index] == active) break;
//				}
//				if(!all && active == Control.ALL) {
//					all = true;
//				} else if(!dba && active == Control.DBA) {
//					dba = true;
//				} else if(index == USER_OPTIONS.length) {
//					return false;
//				}
//			}
//			if(dba) {
//				set(Control.DBA_OPTIONS);
//			} else if(all) {
//				set(Control.USER_OPTIONS);
//			} else {
//				set(actives);
//			}
//			return true;
//		}
//		return false;
//	}




	/**
	 * 增加一个权限
	 */
	public boolean add(int id) {
		return id > 0 && set.add(new Integer(id));
	}

	/**
	 * 增加一组权限
	 * @param list
	 * @return
	 */
	public int add(Collection<Integer> list) {
		int size = set.size();
		set.addAll(list);
		return set.size() - size;
	}

	/**
	 * 增加一组权限
	 * @param ctrl
	 * @return
	 */
	public int add(Control ctrl) {
		return this.add(ctrl.set);
	}

	/**
	 * 删除一个权限
	 * @param id
	 * @return
	 */
	public boolean delete(int id) {
		return set.remove(new Integer(id));
	}

	/**
	 * 删除一组权限
	 * @param list
	 * @return
	 */
	public int delete(Collection<Integer> list) {
		int size = set.size();
		set.removeAll(list);
		return size - set.size();
	}

	/**
	 * 删除一组权限
	 * @param ctrl
	 * @return
	 */
	public int delete(Control ctrl) {
		return delete(ctrl.set);
	}

	/**
	 * 返回权限集合
	 * @return
	 */
	public Collection<Integer> list() {
		return this.set;
	}

	/**
	 * 判断集合是否空
	 * @return
	 */
	public boolean isEmpty() {
		return set.isEmpty();
	}

	/**
	 * 返回集合尺寸
	 * @return
	 */
	public int size() {
		return set.size();
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Control(this);
	}

	/**
	 * 翻译参数
	 * @param id
	 * @return
	 */
	public static String translate(int id) {
		String s = null;
		switch (id) {
		case Control.GRANT:
			s = "GRANT"; break;
		case Control.REVOKE:
			s = "REVOKE"; break;
		case Control.CREATE_USER:
			s = "CREATE USER";	break;
		case Control.DROP_USER:
			s = "DROP USER"; break;
		case Control.ALTER_USER:
			s = "ALTER USER";  break;
		case Control.CREATE_SCHEMA:
			s = "CREATE DATABASE"; break;
		case Control.DROP_SCHEMA:
			s = "DROP DATABASE"; break;
		case Control.CREATE_TABLE:
			s = "CREATE TABLE"; break;
		case Control.DROP_TABLE:
			s = "DROP TABLE"; break;
		case Control.SELECT:
			s = "SELECT"; break;
		case Control.INSERT:
			s = "INSERT"; break;
		case Control.DELETE:
			s = "DELETE"; break;
		case Control.UPDATE:
			s = "UPDATE"; break;
		case Control.CONDUCT:
			s = "CONDUCT"; break;
		case Control.ALL:
			s = "ALL"; break;
		case Control.DBA:
			s = "DBA"; break;
		}
		return s;
	}

	/**
	 * 转换参数
	 * @param s
	 * @return
	 */
	public static int translate(String s) {
		int id = -1;
		if ("CREATE USER".equalsIgnoreCase(s)) {
			id = Control.CREATE_USER;
		} else if ("DROP USER".equalsIgnoreCase(s)) {
			id = Control.DROP_USER;
		} else if("ALTER USER".equalsIgnoreCase(s)) {
			id = Control.ALTER_USER;
		} else if ("GRANT".equalsIgnoreCase(s)) {
			id = Control.GRANT;
		} else if ("REVOKE".equalsIgnoreCase(s)) {
			id = Control.REVOKE;

		} else if ("CREATE DATABASE".equalsIgnoreCase(s)) {
			id = Control.CREATE_SCHEMA;
		} else if("DROP DATABASE".equalsIgnoreCase(s)) {
			id = Control.DROP_SCHEMA;
		} else if ("CREATE TABLE".equalsIgnoreCase(s)) {
			id = Control.CREATE_TABLE;
		} else if("DROP TABLE".equalsIgnoreCase(s)) {
			id = Control.DROP_TABLE;

		}else if ("SELECT".equalsIgnoreCase(s)) {
			id = Control.SELECT;
		} else if ("INSERT".equalsIgnoreCase(s)) {
			id = Control.INSERT;
		} else if ("DELETE".equalsIgnoreCase(s)) {
			id = Control.DELETE;
		} else if ("UPDATE".equalsIgnoreCase(s)) {
			id = Control.UPDATE;
		} else if ("CONDUCT".equalsIgnoreCase(s)) {
			id = Control.CONDUCT;
		} else if ("ALL".equalsIgnoreCase(s)) {
			id = Control.ALL;
		} else if ("DBA".equalsIgnoreCase(s)) {
			id = Control.DBA;
		}
		return id;
	}

}