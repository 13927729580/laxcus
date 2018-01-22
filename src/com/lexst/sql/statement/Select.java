/**
 *
 */
package com.lexst.sql.statement;

import java.io.*;

import com.lexst.sql.schema.*;
import com.lexst.sql.statement.select.*;
import com.lexst.util.*;

public class Select extends Query {
	
	private static final long serialVersionUID = 1L;

	/** 检索范围 */
	private int begin, end;
	
	/** 显示列集合(实际列和函数列) */
	private ShowSheet sheet;// = new ShowSheet();

	/** "Order By" 实例 **/
	private OrderBy orderby;
	
	/** "Group By" 实例 **/
	private GroupBy groupby;

	/**
	 * construct method
	 */
	public Select(int capacity) {
		super(Compute.SELECT_METHOD);
		begin = end = 0;
		if (capacity < 5) {
			capacity = 5;
		}
	}

	/**
	 * default
	 */
	public Select() {
		this(5);
	}

	/**
	 * @param space
	 */
	public Select(Space space) {
		this();
		super.setSpace(space);
	}
	
	/**
	 * 
	 * @param select
	 */
	public Select(Select select) {
		super(select);
		this.setRange(select.begin, select.end);
		if (select.sheet != null) {
			this.sheet = new ShowSheet(select.sheet);
		}
		if (select.orderby != null) {
			this.orderby = new OrderBy(select.orderby);
		}
		if (select.groupby != null) {
			this.groupby = new GroupBy(select.groupby);
		}
	}

	/**
	 * @param begin
	 * @param end
	 */
	public void setRange(int begin, int end) {
		this.begin = begin;
		this.end = end;
	}

	public int getBegin() {
		return this.begin;
	}

	public int getEnd() {
		return this.end;
	}

//	public void setShowId(short[] ids) {
//		if (ids == null || ids.length == 0) {
//			this.realIds = null;
//		} else {
//			realIds = new short[ids.length];
//			for (int i = 0; i < ids.length; i++) {
//				realIds[i] = ids[i];
//			}
//		}
//	}

	public short[] getShowId() {
		return sheet.getShowIds();
	}

	public void setOrderBy(OrderBy object) {
		this.orderby = object;
	}

	public OrderBy getOrderBy() {
		return this.orderby;
	}
	
	/**
	 * 设置显示列集合
	 * @param s
	 */
	public void setShowSheet(ShowSheet s) {
		this.sheet = s;
	}
	
	/**
	 * 返回显示列集合
	 * @return
	 */
	public ShowSheet getShowSheet() {
		return this.sheet;
	}
	
	public void setGroupBy(GroupBy gb) {
		this.groupby = gb;
	}

	public GroupBy getGroupBy() {
		return this.groupby;
	}

	/*
	 * 复制Select对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Select(this);
		
//		Select select = new Select();
//		select.space = new Space(this.space);
//		select.setChunkids(this.chunkIds);
//		select.condition = new Condition(this.condition);
//
//		select.begin = this.begin;
//		select.end = this.end;
//		select.setShowSheet(this.sheet);
////		select.setShowId(this.realIds);
//		select.setOrderBy(this.orderby);
//		select.setGroupBy(this.groupby);
//		return select;
	}

	/**
	 * 需要被抽取的列集合
	 * @return
	 */
	private byte[] buildPickupIds() {
//		short[] columnIds = sheet.getColumnIds();
		short[] columnIds = sheet.getShowIds();
		int items = (columnIds == null ? 0 : columnIds.length);
		ByteArrayOutputStream out = new ByteArrayOutputStream(128);
		byte[] b = Numeric.toBytes(items);
		out.write(b, 0, b.length);
		for (int i = 0; i < items; i++) {
			b = Numeric.toBytes(columnIds[i]);
			out.write(b, 0, b.length);
		}
		byte[] data = out.toByteArray();
		return buildField(Query.COLUMNIDS, data);
	}

	protected int splitColumnIds(byte[] data, int off, int len) {
		int seek = off;
		int end = off + len;
		if(seek + 4 > end) {
			throw new IllegalArgumentException("column identity sizeout!");
		}
		int count = Numeric.toInteger(data, seek, 4);
		seek += 4;
		int size = count * 2;
		if(seek + size > end) {
			throw new IllegalArgumentException("column identity sizeout");
		}
		seek += size;

		return seek - off;
	}

	private byte[] buildOrderBy() {
		if (orderby == null) {
			return null;
		}
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
		OrderBy order = this.orderby;
		while (order != null) {
			byte[] b = Numeric.toBytes(order.getColumnId());
			buff.write(b, 0, b.length);
			buff.write(order.getType());
			order = order.getNext();
		}
		byte[] data = buff.toByteArray();
		return buildField(Query.ORDERBY, data);
	}

	private int splitOrderBy(byte[] data, int offset, int len) {
		int off = offset, end = offset + len;
		while (off < end) {
			short cid = Numeric.toShort(data, off, 2);
			off += 2;
			byte type = data[off];
			off += 1;
			// set order
			OrderBy order = new OrderBy(cid, type);
			this.setOrderBy(order);
		}
		return off - offset;
	}

	/**
	 * @return
	 */
	private byte[] buildRange() {
		if (begin == 0 && end == 0) {
			return null;
		}
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		byte[] b = Numeric.toBytes(begin);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(end);
		buff.write(b, 0, b.length);
		b = buff.toByteArray();
		return buildField(Query.RANGE, b);
	}

	private int splitRange(byte[] data, int off, int len) {
		if (len < 8) {
			throw new IllegalArgumentException("invalid range size");
		}
		int seek = off;
		this.begin = Numeric.toInteger(data, seek, 4);
		seek += 4;
		this.end = Numeric.toInteger(data, seek, 4);
		seek += 4;
		return seek - off;
	}

	private byte[] buildShowSheet() {
		try {
			ByteArrayOutputStream buff = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(buff);
			out.writeObject(this.sheet);
			out.flush();
			byte[] b = buff.toByteArray();
			out.close();
			buff.close();
			return buildField(Query.SHOWSHEET, b);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	private int splitShowSheet(byte[] b, int off, int len) {
		try {
			ByteArrayInputStream buff = new ByteArrayInputStream(b, off, len);
			ObjectInputStream in = new ObjectInputStream(buff);
			this.sheet = (ShowSheet)in.readObject();
			in.close();
			buff.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return len;
	}

	private byte[] buildGroupby() {
		try {
			ByteArrayOutputStream buff = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(buff);
			out.writeObject(this.groupby);
			out.flush();
			byte[] b = buff.toByteArray();
			out.close();
			buff.close();
			return buildField(Query.GROUPBY, b);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	private int splitGroupby(byte[] b, int off, int len) {
		try {
			ByteArrayInputStream buff = new ByteArrayInputStream(b, off, len);
			ObjectInputStream in = new ObjectInputStream(buff);
			this.groupby = (GroupBy)in.readObject();
			in.close();
			buff.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return len;
	}
	
	/**
	 * build
	 *
	 * @return
	 */
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
		// space field
		byte[] b = this.buildSpace();
		buff.write(b, 0, b.length);

		// WHERE比较条件
		b = this.buildCondition();
		buff.write(b, 0, b.length);
		// select column
		b = this.buildPickupIds();
		buff.write(b, 0, b.length);
		// all chunk id
		b = this.buildChunkId();
		buff.write(b, 0, b.length);
		// order by
		b = this.buildOrderBy();
		if (b != null) {
			buff.write(b, 0, b.length);
		}
		// select range
		b = this.buildRange();
		if (b != null) {
			buff.write(b, 0, b.length);
		}
		// show column element(real column and function column)
		b = this.buildShowSheet();
		if(b != null) {
			buff.write(b, 0, b.length);
		}
		// "group by"
		b = this.buildGroupby();
		if(b != null) {
			buff.write(b, 0, b.length);
		}
		// field body
		byte[] data = buff.toByteArray();

		// reset and re-write
		buff.reset();
		int size = 5 + data.length;
		b = Numeric.toBytes(size);
		// all size
		buff.write(b, 0, b.length);
		// select method identity
		buff.write(Query.SELECT_METHOD);
		// write body
		buff.write(data, 0, data.length);

		return buff.toByteArray();
	}

	/**
	 * 解析 SQL SELECT的数据流，返回解析字节流长度
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		if (seek + 5 > off + len) {
			throw new SizeOutOfBoundsException("select stream sizeout!");
		}

		// 数据流总长度(包括自己的4个字节)
		int allsize = Numeric.toInteger(b, seek, 4);
		seek += 4;
		// 数据流截止下标
		int end = off + allsize;
		if (allsize < 1 || end > off + len) {
			throw new SizeOutOfBoundsException("select stream sizeout");
		}
		// 方法名，必须是SELECT
		if (b[seek] != Query.SELECT_METHOD) {
			throw new IllegalArgumentException("invalid select identity!");
		}
		seek += 1;

		// 数据流截止下标
		while (seek < end) {
			Body body = super.splitField(b, seek, end - seek);
			seek += body.length();

			byte[] data = body.data;
			switch (body.id) {
			case Query.SPACE:
				splitSpace(data, 0, data.length); break;
			case Query.CONDITION:
				splitCondition(data, 0, data.length); break;
			case Query.COLUMNIDS:
				splitColumnIds(data, 0, data.length); break;
			case Query.CHUNKS:
				splitChunkId(data, 0, data.length); break;
			case Query.ORDERBY:
				splitOrderBy(data, 0, data.length); break;
			case Query.RANGE:
				splitRange(data, 0, data.length); break;
			case Query.SHOWSHEET:
				splitShowSheet(data, 0, data.length); break;
			case Query.GROUPBY:
				splitGroupby(data, 0, data.length); break;
			}
		}
		return seek - off;
	}

//	public static void main(String[] args) {
//		short cid = 1;
//		//Char ch = new Char(colid, "pentium".getBytes());
//		Raw ch = new Raw(cid, "abcd".getBytes());
//		com.lexst.db.sign.BigSign index = new com.lexst.db.sign.BigSign(0x1020304050607080L, ch);
//		Condition condi = new Condition();
//		condi.setCompare(Condition.EQUAL);
//		condi.setValue(index);
//
//		Raw ch1 = new Raw(cid, "dcba".getBytes());
//		com.lexst.db.sign.BigSign index1 = new com.lexst.db.sign.BigSign(0x8070605040302010L, ch1);
//		Condition sub = new Condition();
//		sub.setCompare(Condition.EQUAL);
//		sub.setValue(index1);
//		condi.addFriend(sub);
//
//		short colid = 1;
//		Order order = new Order( colid++, Order.ASC );
//		Order order2 = new Order( colid++, Order.DESC );
//
//		Space space = new Space("Video", "Word");
//		Select select = new Select(space);
//		select.setCondition(condi);
//		select.setRange(1, 1000);
//		select.setOrder(order);
//		select.setOrder(order2);
//
////		select.setSelectId(new short[] { Short.MIN_VALUE, Short.MAX_VALUE });
////		select.setChunkId(new long[] { Long.MAX_VALUE, Long.MIN_VALUE });
//
//
//		byte[] b = select.build();
//		System.out.printf("select build size %d\n", b.length);
//
//		try {
////			java.io.FileOutputStream out = new java.io.FileOutputStream("c:/select.bin");
////			out.write(b, 0, b.length);
////			out.close();
//			File file = new File("c:/select.bin");
//			b = new byte[(int)file.length()];
//			java.io.FileInputStream in = new java.io.FileInputStream(file);
//			in.read(b);
//			in.close();
//		} catch (java.io.IOException exp) {
//			exp.printStackTrace();
//		}
//
//		Select select2 = new Select();
//		int ret = select2.resolve(b, 0, b.length);
//		System.out.printf("resolve ret is %d\n", ret);
//	}

//	public static void main(String[] args) {
//		short colid = 1;
//		Order order = new Order( colid++, Order.ASC );
//		Order order2 = new Order( colid++, Order.DESC );
//
//		Select select = new Select();
//		select.setOrder(order);
//		select.setOrder(order2);
//
//		byte[] b = select.buildOrderBy();
//		System.out.printf("order byte size %d\n", b.length);
//
//		Select select2 = new Select();
//		int size = select2.splitOrderBy(b, 0, b.length);
//		System.out.printf("split size %d\n", size);
//	}
}