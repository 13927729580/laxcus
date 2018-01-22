/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.split;

import java.io.*;

import com.lexst.util.range.*;

/**
 * @author scott.liang
 * 
 * 字符集分隔器,用于CALL节点. 在"ORDER BY", "GROUP BY" 操作时使用
 * 运行时由CALL节点动态调用, 配置文件写在 "css.xml"配置中
 */
public interface CharsetSpliter extends Serializable {


	LongRange[] split(int sites);

}
