/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.split;

import java.util.*;

import com.lexst.util.range.*;

/**
 * @author scott.liang
 *
 */
public class UTF8Spliter implements CharsetSpliter {

	private static final long serialVersionUID = 1L;
	
	private final static LongRange ASCII = new LongRange(0x0L, 0x7FL);
	private final static LongRange EUROPE = new LongRange(0xC080L, 0xDFBFL);
	private final static LongRange CJK = new LongRange(0xE08080L, 0xEFBFBFL);

	/**
	 * default
	 */
	public UTF8Spliter() {
		super();
	}
	
	/**
	 * UTF8 encode range
	 * @return
	 */
	public long[][] sections() {
		return new long[][] {
			{0x0L, 0x7FL}, // ASCII
			{0xC080L, 0xDFBFL}, //泛欧语系: 希腊, 希伯莱, 斯拉夫(西里尔), 阿拉伯
			{0xE08080L, 0xEFBFBFL}, // CJK, 中日韩
			{0xF0808080L, 0xF7BEBFBF},
			{0xF880808080L, 0xFBBFBFBFBFL},
			{0xFC8080808080L, 0xFDBFBFBFBFBFL}
		};
	}

	/* 
	 * 按照站点数目划分字符集区域 
	 * 分区规则:
	 * 1. 结合可能使用的语言的多少和主机数进行分割
	 * 
	 * @see com.lexst.sql.split.CharsetSpliter#split(int)
	 */
	@Override
	public LongRange[] split(int sites) {
		List<LongRange> array = new ArrayList<LongRange>();
		switch(sites) {
		case 1: // 合为一组
//			array.add(UTF8Spliter.ASCII);
//			array.add(UTF8Spliter.EUROPE);
//			array.add(UTF8Spliter.CJK);
			break;
		case 2:	// ASCII码为一组, 其它字符集合为一组
		case 3:	// ASCII码一组, CJK码一组, 其它字符集一组
		case 4:	// ASCII码, CJK, 西里尔各一组, 其它一组
		case 5: // ASCII码两组, 西里尔一组, CJK一组, 其它各一组
		case 6: // ASCII码两组, 西里尔一组, CJK两组, 其它各一组
		default: //最大化分配ASCII, CJK, 西里尔其次. 其它默认一组
		}
		
		LongRange[] ranges = new LongRange[array.size()];
		return array.toArray(ranges);
	}

}