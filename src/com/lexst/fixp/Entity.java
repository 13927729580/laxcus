/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * basic information set
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/13/2009
 * 
 * @see com.lexst.fixp
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.fixp;

import java.io.*;
import java.util.*;

import com.lexst.remote.*;
import com.lexst.util.host.*;

/**
 * FIXP协议通信过程中传递的数据配置集。<br>
 *
 */
public class Entity implements Cloneable {

	private static final long serialVersionUID = 1589075848193439238L;

	/** 连接方主机地址 **/
	protected SocketHost remote = new SocketHost(SocketHost.NONE);

	/** FIXP命令 **/
	protected Command command;

	/** FIXP消息域集合 **/
	protected ArrayList<Message> array = new ArrayList<Message>(5);

	/** FIXP数据域参数 **/
	protected byte[] data;

	/** 密钥 */
	private Cipher cipher;

	/**
	 * 创建FIXP通信实体
	 */
	public Entity() {
		super();
	}

	/**
	 * 指定消息域集合最小空间
	 * 
	 * @param capacity
	 */
	public Entity(int capacity) {
		super();
		if (capacity < 5) capacity = 5;
		array.ensureCapacity(capacity);
	}

	/**
	 * @param remote
	 */
	public Entity(SocketHost remote) {
		this();
		this.setRemote(remote);
	}

	/**
	 * @param cmd
	 */
	public Entity(Command cmd) {
		this();
		this.setCommand(cmd);
	}

	/**
	 * @param remote
	 * @param cmd
	 */
	public Entity(SocketHost remote, Command cmd) {
		this(remote);
		this.setCommand(cmd);
	}

	/**
	 * @param entity
	 */
	protected Entity(Entity entity) {
		this(entity.array.size());

		// target host
		this.setRemote(entity.remote);
		// command
		this.command = new Command(entity.command);
		// messages
		for (Message msg : entity.array) {
			array.add(new Message(msg));
		}
		// data
		if (entity.data != null && entity.data.length > 0) {
			data = new byte[entity.data.length];
			System.arraycopy(entity.data, 0, data, 0, data.length);
		}
		// cipher
		if (entity.cipher != null) {
			cipher = new Cipher(entity.cipher);
		}
	}

	/**
	 * 设置对方主机地址
	 * 
	 * @param host
	 */
	public void setRemote(SocketHost host) {
		this.remote.set(host);
	}

	/**
	 * 返回对方主机地址
	 * 
	 * @return
	 */
	public SocketHost getRemote() {
		return this.remote;
	}

	/**
	 * 设置FIXP命令
	 * 
	 * @param obj
	 */
	public void setCommand(Command obj) {
		this.command = obj;
	}

	/**
	 * 返回FIXP命令
	 * 
	 * @return
	 */
	public Command getCommand() {
		return this.command;
	}

	public void setCipher(Cipher cipher) {
		this.cipher = cipher;
	}

	public Cipher getCipher() {
		return this.cipher;
	}

	/**
	 * 设置数据域
	 * 
	 * @param b
	 */
	public void setData(byte[] b, int off, int len) {
		if (len > 0) {
			this.data = Arrays.copyOfRange(b, off, off + len);
		} else {
			this.data = null;
		}

		// 设置数据域对等的消息
		if (data == null || data.length == 0) {
			this.replaceMessage(new Message(Key.CONTENT_LENGTH, 0));
		} else {
			this.replaceMessage(new Message(Key.CONTENT_LENGTH, data.length));
		}
	}
	
	/**
	 * 设置数据域
	 * @param b
	 */
	public void setData(byte[] b) {
		setData(b, 0, (b == null ? -1 : b.length));
	}

	/**
	 * 设置数据域
	 * 
	 * @param objects
	 */
	public void setData(Object[] objects) throws IOException {
		byte[] b = Apply.build(objects);
		if (b == null) {
			throw new IllegalArgumentException("invalid objects");
		}
		this.setData(b, 0, b.length);
		// 序列化对象宿主(目前只限JAVA语言)
		this.addMessage(new Message(Key.SERIAL_TYPE, "java"));
		// 数据域的对象成员尺寸
		this.addMessage(new Message(Key.SERIAL_OBJECTS, objects.length));
	}

	/**
	 * 返回数据域
	 * 
	 * @return
	 */
	public byte[] getData() {
		return this.data;
	}

	/**
	 * 添加一个消息
	 * 
	 * @param message
	 */
	public void addMessage(Message message) {
		array.add(message);
	}

	/**
	 * 查找一个消息，从同值消息序列的0下标开始查找
	 * 
	 * @param key
	 * @return
	 */
	public Message findMessage(short key, int index) {
		if (index < 0) {
			throw new IllegalArgumentException("invalid index range");
		}
		int count = 0;
		for (int i = 0; i < array.size(); i++) {
			Message msg = array.get(i);
			if (msg.getKey() == key) {
				if (count == index)
					return msg;
				count++;
			}
		}
		return null;
	}

	/**
	 * find a message
	 * 
	 * @param key
	 * @return
	 */
	public Message findMessage(short key) {
		return findMessage(key, 0);
	}

	public byte[] findBinary(short key) {
		return findRaw(key, 0);
	}

	public byte[] findBinary(short key, int index) {
		return findRaw(key, index);
	}

	public byte[] findRaw(short key, int index) {
		Message msg = findMessage(key, index);
		if (msg == null || !msg.isBinary()) {
			return null;
		}
		return msg.getValue();
	}

	public Boolean findBoolean(short key) {
		return findBoolean(key, 0);
	}

	public Boolean findBoolean(short key, int index) {
		Message msg = findMessage(key, index);
		if (msg == null || !msg.isBoolean()) {
			return null;
		}
		return msg.booleanValue();
	}

	public String findChar(short key) {
		return findChar(key, 0);
	}

	public String findChar(short key, int index) {
		Message msg = findMessage(key, index);
		if (msg == null || !msg.isChar()) {
			return null;
		}
		return msg.stringValue();
	}

	public String findChar(short key, String encode) {
		return findChar(key, encode, 0);
	}

	public String findChar(short key, String encode, int index) {
		Message msg = findMessage(key, index);
		if (msg == null || !msg.isChar()) {
			return null;
		}
		try {
			byte[] b = msg.getValue();
			return new String(b, 0, b.length, encode);
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		return null;
	}

	public Short findShort(short key) {
		return findShort(key, 0);
	}

	public Short findShort(short key, int index) {
		Message msg = findMessage(key, index);
		if (msg == null || !msg.isShort()) {
			return null;
		}
		return msg.shortValue();
	}

	public Integer findInt(short key) {
		return findInt(key, 0);
	}

	public Integer findInt(short key, int index) {
		Message msg = findMessage(key, index);
		if (msg == null || !msg.isInt()) {
			return null;
		}
		return msg.intValue();
	}

	public Long findLong(short key) {
		return findLong(key, 0);
	}

	public Long findLong(short key, int index) {
		Message msg = findMessage(key, index);
		if (msg == null || !msg.isLong()) {
			return null;
		}
		return msg.longValue();
	}

	public Float findFloat(short key) {
		return findFloat(key, 0);
	}

	public Float findFloat(short key, int index) {
		Message msg = findMessage(key, index);
		if (msg == null || !msg.isFloat()) {
			return null;
		}
		return msg.floatValue();
	}

	public Double findDouble(short key) {
		return findDouble(key, 0);
	}

	public Double findDouble(short key, int index) {
		Message msg = findMessage(key, 0);
		if (msg == null || !msg.isDouble()) {
			return null;
		}
		return msg.doubleValue();
	}

	public void addMessage(short key, byte[] value) {
		Message msg = new Message(key, value);
		this.addMessage(msg);
	}

	public void addMessage(short key, String value) {
		Message msg = new Message(key, value);
		this.addMessage(msg);
	}

	public void addMessage(short key, boolean value) {
		Message msg = new Message(key, value);
		this.addMessage(msg);
	}

	public void addMessage(short key, short value) {
		Message msg = new Message(key, value);
		addMessage(msg);
	}

	public void addMessage(short key, int value) {
		Message msg = new Message(key, value);
		this.addMessage(msg);
	}

	public void addMessage(short key, long value) {
		Message msg = new Message(key, value);
		this.addMessage(msg);
	}

	public void addMessage(short key, float value) {
		Message msg = new Message(key, value);
		this.addMessage(msg);
	}

	public void addMessage(short key, double value) {
		Message msg = new Message(key, value);
		this.addMessage(msg);
	}

	/**
	 * 替换消息
	 * 
	 * @param message
	 */
	public void replaceMessage(Message message) {
		this.removeMessage(message.getKey());
		array.add(message);
	}

	/**
	 * 根据键值删除对应消息
	 * 
	 * @param key
	 */
	public void removeMessage(short key) {
		for (int index = 0; index < array.size(); index++) {
			Message msg = array.get(index);
			if (msg.getKey() == key) {
				array.remove(index);
				index--;
			}
		}
	}

	/**
	 * 返回消息集合
	 * 
	 * @return
	 */
	public List<Message> messages() {
		return array;
	}

	/**
	 * 收缩内存到有效范围
	 */
	public void trimMessages() {
		this.array.trimToSize();
	}

	/**
	 * build entity head
	 * 
	 * @param datalen
	 * @return
	 */
	public byte[] buildHead(int datalen) {
		replaceMessage(new Message(Key.CONTENT_LENGTH, datalen));
		// set message size
		command.setMessageCount((short) array.size());
		// command to byte, write to buff
		byte[] b = command.build();
		ByteArrayOutputStream buff = new ByteArrayOutputStream(512);
		buff.write(b, 0, b.length);
		// message to byte, write to buff
		for (Message msg : array) {
			b = msg.build();
			buff.write(b, 0, b.length);
		}
		return buff.toByteArray();
	}

	/**
	 * build entity packet
	 * 
	 * @param datalen
	 *            (data content length)
	 * @return
	 * @throws FixpProtocolException
	 */
	public byte[] build(int datalen) {
		replaceMessage(new Message(Key.CONTENT_LENGTH, datalen));
		// set message size
		command.setMessageCount((short) array.size());
		// command to byte, write to buff
		byte[] b = command.build();
		ByteArrayOutputStream buff = new ByteArrayOutputStream(10240);
		buff.write(b, 0, b.length);
		// message to byte, write to buff
		for (Message msg : array) {
			b = msg.build();
			buff.write(b, 0, b.length);
		}
		// write data to buff
		if (data != null && data.length > 0) {
			buff.write(data, 0, data.length);
		}
		return buff.toByteArray();
	}

	/**
	 * 
	 * @return
	 * @throws FixpProtocolException
	 */
	public byte[] build() {
		return build(data == null ? 0 : data.length);
	}

	/**
	 * resolve fixp data
	 * 
	 * @param b
	 * @return
	 */
	public boolean resolve(byte[] b) {
		command = new Command();
		command.resolve(b);

		int off = Command.COMMAND_SIZE;
		int msg_count = command.getMessageCount();
		for (int i = 0; i < msg_count; i++) {
			Message msg = new Message();
			int size = msg.resolve(b, off);
			if (size < 1)
				break;
			off += size;
			this.addMessage(msg);
		}
		int len = getContentLength();
		if (len > 0) {
			data = new byte[len];
			System.arraycopy(b, off, data, 0, len);
		}
		return true;
	}

	public int getContentLength() {
		Message msg = findMessage(Key.CONTENT_LENGTH);
		if (msg == null)
			return -1;
		return msg.intValue();
	}

	public void setContentItems(long items) {
		this.replaceMessage(new Message(Key.CONTENT_ITEMS, items));
	}

	public long getContentItems() {
		Message msg = findMessage(Key.CONTENT_ITEMS);
		if (msg == null)
			return -1;
		return msg.longValue();
	}
}