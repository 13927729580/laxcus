/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct;

/**
 * 
 *
 */
public class ToInputObject extends InputObject {

	private static final long serialVersionUID = -4403989651970803058L;
	
	/** 下一级"aggregate"操作对象 */
	private ToInputObject slave;

	/**
	 * default
	 */
	public ToInputObject() {
		super();
	}
	
	/**
	 * @param naming
	 */
	public ToInputObject(String naming) {
		this();
		this.setNaming(naming);
	}

	/**
	 * @param in
	 */
	public ToInputObject(ToInputObject object) {
		super(object);
		if (object.slave != null) {
			this.slave = new ToInputObject(object.slave);
		}
	}

	/**
	 * 设置对象到最后
	 * @param object
	 */
	public void setLast(ToInputObject object) {
		if(slave != null) {
			slave.setLast(object);
		} else {
			slave = object;
		}
	}
	
	/**
	 * 是否有子级对象
	 * @return
	 */
	public boolean hasNext() {
		return this.slave != null;
	}

	/**
	 * 返回子级ToInputObject对象
	 * 
	 * @return
	 */
	public ToInputObject next() {
		return this.slave;
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.sql.distribute.NamingObject#duplicate()
	 */
	@Override
	public Object duplicate() {
		return new ToInputObject(this);
	}

}