/**
 * 
 */
package com.lexst.data.pool;

import com.lexst.sql.schema.*;

public class EntityIdentity implements Comparable<EntityIdentity> {

	private Space space;
	private long chunkid;
	
	/**
	 * 
	 */
	public EntityIdentity(String schema, String table, long id) {
		space = new Space(schema, table);
		chunkid = id;
	}

	public Space getSpace() {
		return space;
	}
	
	public long getChunkId() {
		return chunkid;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != EntityIdentity.class) {
			return false;
		} else if (object == this) {
			return true;
		}

		return this.compareTo((EntityIdentity) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (space.hashCode() ^ chunkid);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(EntityIdentity identity) {
		int ret = (chunkid < identity.chunkid ? -1 : (chunkid > identity.chunkid ? 1 : 0));
		if (ret == 0) {
			ret = space.compareTo(identity.space);
		}
		return ret;
	}
}