package com.lexst.log.client;

import com.lexst.util.lock.*;

public final class LogBuffer {

	private SingleLock lock = new SingleLock();
	
	/** 字符串缓存，最小10K字节长度 **/
	private StringBuilder buff = new StringBuilder(10240);

	/**
	 * construct method
	 */
	public LogBuffer() {
		super();
	}

	/**
	 * 将内存空间调整为指定大小
	 * 
	 * @param capacity - 指定内存尺寸
	 */
	public boolean ensure(int capacity) {
		if (capacity > 10240) {
			buff.ensureCapacity(capacity);
			return true;
		}
		return false;
	}

	public int capacity() {
		return buff.capacity();
	}

	/**
	 * check size out
	 * @return
	 */
	public boolean isFull() {
		int len = buff.length();
		return len + 2048 >= buff.capacity();
	}

	/**
	 * append log string
	 * @param log
	 */
	public void append(String log) {
		lock.lock();
		try {
			if (log != null) {
				buff.append(log);
			}
		} catch (Throwable exp) {

		} finally {
			lock.unlock();
		}
	}

	public boolean isEmpty() {
		return buff.length() == 0;
	}

	/**
	 * return current log size
	 * @return
	 */
	public int length() {
		return buff.length();
	}

	/**
	 * clear log data
	 */
	public void clear() {
		lock.lock();
		try {
			if (buff.length() > 0) {
				buff.delete(0, buff.length());
			}
		} catch (Throwable exp) {

		} finally {
			lock.unlock();
		}
	}

	/**
	 * get log data, and clear log data
	 * @return String
	 */
	public String remove() {
		String s = "";
		lock.lock();
		try {
			if (buff.length() > 0) {
				s = buff.toString();
				buff.delete(0, buff.length());
			}
		} catch (Throwable exp) {
			
		} finally {
			lock.unlock();
		}
		return s;
	}
	
	/**
	 * get log data, and clear log data
	 * @return String
	 */
	public String flush() {
		String s = "";
		lock.lock();
		try {
			if (buff.length() > 0) {
				s = buff.toString();
			}
		} catch (Throwable exp) {

		} finally {
			lock.unlock();
		}
		return s;
	}
}