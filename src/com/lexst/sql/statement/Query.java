/**
 *
 */
package com.lexst.sql.statement;

import java.io.*;
import java.util.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.sql.index.*;
import com.lexst.util.*;

/**
 * 该类继承自SQLComputing，是SQL标准的SELECT、DELETE、UPDATE语句的基类
 */
public class Query extends SQLMethod {
	
	private static final long serialVersionUID = -5195502216526408486L;

	/** 被检索的chunkid集合(chunkid不允许重复) **/
	private Set<java.lang.Long> array = new TreeSet<java.lang.Long>();

	/** SQL WHERE 检索条件 **/
	protected Condition condition;

	/**
	 * default
	 */
	protected Query() {
		super();
	}

	/**
	 * @param method
	 */
	protected Query(byte method) {
		super(method);
	}
	
	/**
	 * @param query
	 */
	protected Query(Query query) {
		super(query);
		this.setChunkids(query.array);
		if (query.condition != null) {
			this.condition = new Condition(query.condition);
		}
	}
	
	/**
	 * 增加一批chunkid
	 * 
	 * @param chunkids
	 */
	public void addChunkids(long[] chunkids) {
//		if (chunkIds == null) {
//			chunkIds = new long[all.length];
//			System.arraycopy(all, 0, chunkIds, 0, all.length);
//		} else {
//			long[] a = new long[chunkIds.length];
//			System.arraycopy(chunkIds, 0, a, 0, chunkIds.length);
//
//			chunkIds = new long[a.length + all.length];
//			System.arraycopy(a, 0, chunkIds, 0, a.length);
//			System.arraycopy(all, 0, chunkIds, a.length, all.length);
//		}
		
		for (int i = 0; chunkids != null && i < chunkids.length; i++) {
			array.add(chunkids[i]);
		}
	}

	/**
	 * 设置一批chunkid
	 * 
	 * @param set
	 */
	public void setChunkids(Collection<java.lang.Long> set) {
//		if (all == null || all.isEmpty()) {
//			chunkIds = null;
//		} else {
//			chunkIds = new long[all.size()];
//			int index = 0;
//			for (java.lang.Long value : all) {
//				chunkIds[index++] = value.longValue();
//			}
//		}
		
		if (set == null || set.isEmpty()) {
			array.clear();
		} else {
			array.addAll(set);
		}
	}

	/**
	 * 设置一批chunkid
	 * 
	 * @param chunkid
	 */
	public void setChunkids(long[] chunkid) {
//		if (all == null) {
//			chunkIds = null;
//		} else {
//			chunkIds = new long[all.length];
//			for (int i = 0; i < all.length; i++) {
//				chunkIds[i] = all[i];
//			}
//		}
		
		if (chunkid == null) {
			array.clear();
		} else {
			for (int i = 0; i < chunkid.length; i++) {
				array.add(chunkid[i]);
			}
		}
	}

	/**
	 * 返回全部chunkid
	 * 
	 * @return
	 */
	public long[] getChunkids() {
		int size = array.size();
		if (size == 0) {
			return null;
		}
		
		long[] chunkids = new long[size];
		java.util.Iterator<java.lang.Long> iterator = array.iterator();
		for (int i = 0; i < chunkids.length; i++) {
			chunkids[i] = iterator.next();
		}
		return chunkids;
	}

	/**
	 * 设置WHERE检索条件，以链表的形式设置
	 * @param condi
	 */
	public void setCondition(Condition condi) {
		if (this.condition == null) {
			this.condition = condi;
		} else {
			condition.setLast(condi);
		}
	}

	/**
	 * 返回 WHERE检索条件
	 * @return
	 */
	public Condition getCondition() {
		return this.condition;
	}

//	/**
//	 * combin a condition
//	 * @param condi
//	 * @return
//	 */
//	private byte[] combin_condition1(Condition condi) {
//		ByteArrayOutputStream buff = new ByteArrayOutputStream();
//		// 1. outside relations
//		buff.write(condi.getOutsideRelation());
//		// 2. previous relations
//		buff.write(condi.getRelation());
//		// 3. compare symbol
//		buff.write(condi.getCompare());
//		// 4. index type
//		WhereIndex whereIndex = condi.getValue();
//		byte indexType = whereIndex.getType();
//		buff.write(indexType);
//		// 5. index serial
//		byte[] b = null;
//		switch(indexType) {
//		case Type.SHORT_INDEX: {
//			com.lexst.sql.index.ShortIndex index = (com.lexst.sql.index.ShortIndex) whereIndex;
//			b = Numeric.toBytes(index.getValue());
//		}
//			break;
//		case Type.INTEGER_INDEX: {
//			com.lexst.sql.index.IntegerIndex index = (com.lexst.sql.index.IntegerIndex)whereIndex;
//			b = Numeric.toBytes(index.getValue());
//		}
//			break;
//		case Type.LONG_INDEX: {
//			com.lexst.sql.index.LongIndex index = (com.lexst.sql.index.LongIndex)whereIndex;
//			b = Numeric.toBytes(index.getValue());
//		}
//			break;
//		case Type.FLOAT_INDEX: {
//			com.lexst.sql.index.FloatIndex index = (com.lexst.sql.index.FloatIndex)whereIndex;
//			float num = index.getValue();
//			b = Numeric.toBytes( java.lang.Float.floatToIntBits(num) );
//		}
//			break;
//		case Type.DOUBLE_INDEX: {
//			com.lexst.sql.index.DoubleIndex index = (com.lexst.sql.index.DoubleIndex)whereIndex;
//			double num = index.getValue();
//			b = Numeric.toBytes(java.lang.Double.doubleToLongBits(num));
//		}
//			break;
//		default:
//			throw new java.lang.IllegalArgumentException("invalid index column");
//		}
//		buff.write(b, 0, b.length);
//		//6. column identity
//		Column column = whereIndex.getColumn();
//		short cid = column.getId();
//		b = Numeric.toBytes(cid);
//		buff.write(b, 0, b.length);
//		
//		//7. column data
////		ByteArrayOutputStream head = new ByteArrayOutputStream();
////		ByteArrayOutputStream body = new ByteArrayOutputStream();
////		column.build(head, body);
////		b = body.toByteArray();		
////		head.write(b, 0, b.length);
////		b = head.toByteArray();
////		buff.write(b, 0, b.length);
//
//		ByteArrayOutputStream stream = new ByteArrayOutputStream();
//		column.build(stream);
//		b = stream.toByteArray();
//		buff.write(b, 0, b.length);
//		
//		// friend condition
//		ByteArrayOutputStream out = new ByteArrayOutputStream();
//		for (Condition sub : condi.getPartners()) {
//			b = buildCondition(sub);
//			out.write(b, 0, b.length);
//		}
//		byte[] data = out.toByteArray();
//		int friendSize = (data == null ? 0 : data.length);
//		b = Numeric.toBytes(friendSize);
//		buff.write(b, 0, b.length);
//		if (friendSize > 0) {
//			buff.write(data, 0, data.length);
//		}
//		// friend condition list
//		return buff.toByteArray();
//	}

	/**
	 * combin a condition
	 * @param condi
	 * @return
	 */
	private byte[] buildCondition(Condition condi) {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		// 1. 外部连接关系 (AND|OR)
		buff.write(condi.getOutsideRelation());
		// 2. 同级前面条件连接关系(AND|OR)
		buff.write(condi.getRelation());
		// 3. 值比较关系
		buff.write(condi.getCompare());
		// 4. 生成WhereIndex数据流，写入数据流长度和数据
		byte[] data = condi.getValue().build();
		byte[] b = Numeric.toBytes(data.length);
		// 数据流长度
		buff.write(b, 0, b.length);
		// 数据
		buff.write(data, 0, data.length);
		
		// friend condition
		ByteArrayOutputStream partners = new ByteArrayOutputStream();
		for (Condition partner : condi.getPartners()) {
			data = buildCondition(partner);
			partners.write(data, 0, data.length);
		}
		data = partners.toByteArray();
		int partnerSize = (data == null ? 0 : data.length);
		b = Numeric.toBytes(partnerSize);
		buff.write(b, 0, b.length);
		if (partnerSize > 0) {
			buff.write(data, 0, data.length);
		}
		// 数据流
		return buff.toByteArray();
	}

	/**
	 * 生成WHERE比较关系数据流
	 * @return
	 */
	protected byte[] buildCondition() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);

		Condition condi = this.condition;
		while (condi != null) {
			byte[] b = buildCondition(condi);
			buff.write(b, 0, b.length);
			// next condition
			condi = condi.getNext();
		}
		byte[] data = buff.toByteArray();
		return buildField(Query.CONDITION, data);
	}

//	private int splitCondition1(byte[] b, int off, int len, boolean friend) {
//		int seek = off;
//		int end = off + len;
//		Condition condi = new Condition();
//
//		// outside relate
//		condi.setOutsideRelation(b[seek++]);
//		// relate
//		condi.setRelation(b[seek++]);
//		// compare
//		condi.setCompare(b[seek++]);
//		
//		// condition type
//		byte type = b[seek++];
//
//		WhereIndex index = null;
//		switch(type) {
//		case Type.SHORT_INDEX: {
//			short value = Numeric.toShort(b, seek, 2);
//			seek += 2;
//			index = new com.lexst.sql.index.ShortIndex(value);
//		}
//			break;
//		case Type.INTEGER_INDEX: {
//			int value = Numeric.toInteger(b, seek, 4);
//			seek += 4;
//			index = new com.lexst.sql.index.IntegerIndex(value);
//		}
//			break;
//		case Type.LONG_INDEX: {
//			long value = Numeric.toLong(b, seek, 8);
//			seek += 8;
//			index = new com.lexst.sql.index.LongIndex(value);
//		}
//			break;
//		case Type.FLOAT_INDEX: {
//			int value = Numeric.toInteger(b, seek, 4);
//			seek += 4;
//			float num = java.lang.Float.intBitsToFloat(value);
//			index = new com.lexst.sql.index.FloatIndex(num);
//		}
//			break;
//		case Type.DOUBLE_INDEX: {
//			long bits = Numeric.toLong(b, seek, 8);
//			seek += 8;
//			double num = java.lang.Double.longBitsToDouble(bits);
//			index = new com.lexst.sql.index.DoubleIndex(num);
//		}
//			break;
//		default:
//			throw new IllegalArgumentException("invalid index number");
//		}
//		// column 2
//		short columnId = Numeric.toShort(b, seek, 2);
//		seek += 2;
//		
//		
//		// column
//		Column column = null;		
//		byte columnType = Type.parseType(b[seek]);
//		
//		switch(columnType) {
//		case Type.RAW:
//			column = new Raw(); break;
//		case Type.CHAR:
//			column = new Char(); break;
//		case Type.SCHAR:
//			column = new SChar(); break;
//		case Type.WCHAR:
//			column = new WChar(); break;
//			
//		case Type.VCHAR:
//			column = new VChar(); break;
//		case Type.VSCHAR:
//			column = new VSChar(); break;
//		case Type.VWCHAR:
//			column = new VWChar(); break;
//			
//		case Type.SHORT:
//			column = new com.lexst.sql.column.Short(); break;
//		case Type.INTEGER:
//			column = new com.lexst.sql.column.Integer(); break;
//		case Type.LONG:
//			column = new com.lexst.sql.column.Long(); break;
//		case Type.FLOAT:
//			column = new com.lexst.sql.column.Float(); break;
//		case Type.DOUBLE:
//			column = new com.lexst.sql.column.Double(); break;
//		case Type.DATE:
//			column = new com.lexst.sql.column.Date(); break;
//		case Type.TIME:
//			column = new com.lexst.sql.column.Time(); break;
//		case Type.TIMESTAMP:
//			column = new com.lexst.sql.column.Timestamp(); break;
//		default:
//			throw new IllegalArgumentException("invalid column");
//		}
//
//		column.setId(columnId);
//		int length = column.resolve(b, seek, end - seek);
//		seek += length;
//		index.setColumn(column);
//		condi.setValue(index);
//		
//		// if firend conditon
//		if (friend) {
//			Condition head = getCondition().getLast();
//			head.addPartner(condi);
//		} else {
//			this.setCondition(condi);
//		}
//
//		// split firend condtion
//		int friendSize = Numeric.toInteger(b, seek, 4);
//		seek += 4;
//
//		for (int count = 0; count < friendSize;) {
//			int size = splitCondition(b, seek, friendSize - count, true);
//			count += size;
//			seek += size;
//		}
//
//		return seek - off;
//	}

	/**
	 * 解析condition条件数据流
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @param friend
	 * @return
	 */
	private int splitCondition(byte[] b, int off, int len, boolean friend) {
		int seek = off;
		int end = off + len;
		if (seek + 8 > end) {
			throw new SizeOutOfBoundsException("condition sizeout!");
		}
		
		Condition condi = new Condition();
		// 外部关联关系
		condi.setOutsideRelation(b[seek]);
		seek += 1;
		// 同级前面的关联关系
		condi.setRelation(b[seek]);
		seek += 1;
		//  当前值的比较关系
		condi.setCompare(b[seek]);
		seek += 1;
		// WhereIndex数据流长度
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		WhereIndex index = null;
		switch(b[seek]) {
		case Type.SHORT_INDEX:
			index = new com.lexst.sql.index.ShortIndex(); break;
		case Type.INTEGER_INDEX:
			index = new com.lexst.sql.index.IntegerIndex(); break;
		case Type.LONG_INDEX:
			index = new com.lexst.sql.index.LongIndex(); break;
		case Type.FLOAT_INDEX:
			index = new com.lexst.sql.index.FloatIndex(); break;
		case Type.DOUBLE_INDEX:
			index = new com.lexst.sql.index.DoubleIndex(); break;
		default:
			throw new ColumnException("cannot support index type:%d", b[seek]);
		}
		
		size = index.resolve(b, seek, size);
		seek += size;
		condi.setValue(index);	
		
		// 如果是同级连接关系
		if (friend) {
			Condition last = getCondition().getLast();
			last.addPartner(condi);
		} else {
			this.setCondition(condi);
		}

		// 解析同级连接条件数据流
		int partnerSize = Numeric.toInteger(b, seek, 4);
		seek += 4;

		for (int count = 0; count < partnerSize;) {
			size = splitCondition(b, seek, partnerSize - count, true);
			count += size;
			seek += size;
		}

		return seek - off;
	}
	
	/**
	 * 解析condition
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	protected int splitCondition(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		while (seek < end) {
			int size = splitCondition(b, seek, end - seek, false);
			seek += size;
		}
		return seek - off;
	}

	/**
	 * 生成chunkid数据流
	 * 
	 * @return
	 */
	protected byte[] buildChunkId() {
		long[] chunkIds = this.getChunkids();
		int size = (chunkIds == null ? 0 : chunkIds.length);

		ByteArrayOutputStream out = new ByteArrayOutputStream(4 + 8 * size);

		byte[] b = Numeric.toBytes(size);
		out.write(b, 0, b.length);

		for (int i = 0; i < size; i++) {
			b = Numeric.toBytes(chunkIds[i]);
			out.write(b, 0, b.length);
		}
		byte[] data = out.toByteArray();
		return this.buildField(Query.CHUNKS, data);
	}

	protected int splitChunkId(byte[] b, int off, int len) {
		array.clear();
		
		int seek = off;
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (size > 0) {
//			this.chunkIds = new long[size];
//			for (int i = 0; i < size; i++) {
//				chunkIds[i] = Numeric.toLong(data, seek, 8);
//				seek += 8;
//			}
			
			for(int i = 0; i < size; i++) {
				long chunkid = Numeric.toLong(b, seek, 8);
				seek += 8;
				array.add(chunkid);
			}
		}
		return seek - off;
	}

}