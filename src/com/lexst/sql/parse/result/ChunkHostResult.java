/**
 * 
 */
package com.lexst.sql.parse.result;

import java.util.*;
import com.lexst.site.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;

public class ChunkHostResult {

	private String schema;

	private Space space;

	private int type;
	
	private List<Address> array = new ArrayList<Address>();

	/**
	 * 
	 */
	private ChunkHostResult() {
		super();
		this.type = 0;
	}

	public ChunkHostResult(String schema) {
		this();
		this.setSchema(schema);
	}
	
	public ChunkHostResult(Space space) {
		this();
		this.setSpace(space);
	}


	public ChunkHostResult(String schema, int site, List<Address> sites) {
		this();
		this.setSchema(schema);
		this.addAddresses(site, sites);
	}

	
	public ChunkHostResult(Space space, int site, List<Address> sites) {
		this();
		this.setSpace(space);
		this.addAddresses(site, sites);
	}

	public String getSchema() {
		return this.schema;
	}

	public void setSchema(String s) {
		this.schema = s;
	}

	public void setSpace(Space s) {
		this.space = new Space(s);
	}

	public Space getSpace() {
		return this.space;
	}

	public void addAddresses(int type, List<Address> sites) {
		if (type != Site.DATA_SITE && type != Site.HOME_SITE) {
			throw new IllegalArgumentException("only datasite or homesite");
		}
		this.type = type;
		array.addAll(sites);
	}
	
	public Address[] getAddresses() {
		Address[] s = new Address[array.size()];
		return array.toArray(s);
	}

	public int getHostType() {
		return this.type;
	}

	public boolean isDataSite() {
		return this.type == Site.DATA_SITE;
	}

	public boolean isHomeSite() {
		return this.type == Site.HOME_SITE;
	}

}