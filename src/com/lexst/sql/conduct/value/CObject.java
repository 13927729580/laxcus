package com.lexst.sql.conduct.value;

import java.io.*;
import java.util.*;

import com.lexst.util.*;

public class CObject extends CValue {
	
	private static final long serialVersionUID = -5358388638793420063L;
	
	private Object value;

	/**
	 * default
	 */
	protected CObject() {
		super(CValue.OBJECT);
	}
	
	/**
	 * @param name
	 * @param value
	 */
	public CObject(String name, Object value) {
		this();
		this.setName(name);
		this.setValue(value);
	}

	/**
	 * 复制对象
	 * @param param
	 */
	public CObject(CObject param) {
		super(param);
		setValue(param.value);
	}
	
	public void setValue(Object obj) {
		this.value = obj;
	}
	
	public Object getValue() {
		return this.value;
	}

	@Override
	public CValue duplicate() {
		return new CObject(this);
	}

	@Override
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		super.buildTag(buff);
		
		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream(10240);
			ObjectOutputStream out = new ObjectOutputStream(bout);
			out.writeObject(value);
			out.close();
			
			byte[] value = bout.toByteArray();
			byte[] b = Numeric.toBytes(value.length);
			buff.write(b, 0, b.length);
			buff.write(value, 0, value.length);

		} catch (IOException exp) {
			exp.printStackTrace();
		} catch (Throwable exp) {
			exp.printStackTrace();
		}
		return buff.toByteArray();
	}

	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		int size = super.resolveTag(b, seek, len);
		seek += size;

		if (seek + 4 > end) {
			throw new IndexOutOfBoundsException("raw indexout");
		}
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;

		if (seek + size > end) {
			throw new IndexOutOfBoundsException("raw indexout");
		}

		byte[] bis = Arrays.copyOfRange(b, seek, seek + size);
		seek += size;

		try {
			ByteArrayInputStream bin = new ByteArrayInputStream(bis);
			ObjectInputStream in = new ObjectInputStream(bin);
			value = in.readObject();
		} catch (IOException exp) {
			exp.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return seek - off;
	}

}
