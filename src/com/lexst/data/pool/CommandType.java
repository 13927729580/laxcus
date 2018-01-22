/**
 * 
 */
package com.lexst.data.pool;

import com.lexst.sql.schema.*;


final class CommandType {

	public final static int REBUILD = 1;
	public final static int LOAD_INDEX = 2;
	public final static int STOP_INDEX = 3;
	public final static int LOAD_CHUNK = 4;
	public final static int STOP_CHUNK = 5;

	int type;
	Space space;
	short columnId;

	/**
	 * @param type
	 * @param space
	 */
	public CommandType(int type, Space space) {
		super();
		this.setType(type);
		this.setSpace(space);
		this.columnId = 0; // default is 0
	}

	/**
	 * @param type
	 * @param space
	 * @param id
	 */
	public CommandType(int type, Space space, short id) {
		this(type, space);
		this.setColumnId(id);
	}
	
	public void setType(int i) {
		this.type = i;
	}

	public int getType() {
		return this.type;
	}

	public void setSpace(Space s) {
		space = new Space(s);
	}

	public Space getSpace() {
		return space;
	}
	
	public void setColumnId(short id) {
		this.columnId = id;
	}
	
	public short getColumnId() {
		return this.columnId;
	}
}
