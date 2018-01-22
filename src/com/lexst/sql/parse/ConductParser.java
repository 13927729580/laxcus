/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.parse;

import java.util.regex.*;

import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.value.*;
import com.lexst.sql.statement.*;

/**
 * 解析分布计算Conduct命令<br><br>
 * 
 * Conduct是异步并行处理过程，用于大规模、分布式的数据计算<br><br>
 * 
 * Conduct处理流程: call nodes -> data nodes(return) -> call nodes(resolve area) -> work nodes(return) -> call nodes(resolve Directarea) -> next work nodes(return) -> call nodes<br>
 * 
 * 命名单元标准排列顺序依次是: init、from、balance、to(含subto)、collect，也可以打乱排列。<br>
 * "balance、collect"命名是可选定义，"init"，"from"和"to"是必须定义。<br><br>
 * 
 * init操作发生在call节点，其作用是为后续计算分配资源，定义规则。<br>
 * from操作作用在data节点，是算法的"diffuse"阶段，允许多个"query"字段，即可以同时向多个表进行检索。<br>
 * to操作中包括subto操作，是链接/迭代关系，作用在work节点，是算法的"aggregate"阶段。任务运行由CALL节点负责。<br><br>
 * 
 * 
 * 命名单元在"xxx naming:xxx"之后是参数域。参数包括系统参数和自定义参数两部分。系统参数的名称和值类型固定，自定义参数的名称和值都由用户定义。<br>
 * 系统参数和自定义参数的区别是: 系统参数名称和值之间以冒号':'分隔; 自定义参数名称和值之间以等号'='分隔，自定义参数同时指定数据类型。<br>
 * 系统参数的字符串类型由双引号包括，自定义参数的字符串类型由单引号包括<br>
 * 目前已经定义的系统参数有: sites, query, writeto <br><br>
 * 
 * 
 * 自定义参数格式: key_name(data_type)=value,key2_name(data_type)=value,....<br>
 * 如pid(int)=122, seg_word(string)='helo', today(timestamp)='2012-12-3 8:12:32 122'。<br>
 * 参数值格式与SQL标准一致。<br>
 * 自定义参数名称允许重复。<br>
 * 
 */

public class ConductParser extends SQLParser { //extends DistributeParser {

	/** CONDUCT语法 **/
	private final static String CONDUCT_PREFIX = "^\\s*(?i)(?:CONDUCT)\\s+(.+)$";
	private final static String CONDUCT_SEGMENTS = "^\\s*((?i)(INIT|FROM|BALANCE|TO|COLLECT)\\s+(?i)(?:NAMING)\\s*\\:.+?)(\\s+(?i)(?:INIT|FROM|BALANCE|TO|COLLECT)\\s+(?i)(?:NAMING)\\s*\\:.+|\\s*)$";

	/** 公共子任务命名 **/
	private final static String INIT_SYNTAX = "^\\s*(?i)(?:INIT\\s+NAMING)\\s*\\:\\s*([\\w]+[\\w\\-]+[\\w]+)(\\s+.+|\\s*)$";
	private final static String FROM_SYNTAX = "^\\s*(?i)(?:FROM\\s+NAMING)\\s*\\:\\s*([\\w]+[\\w\\-]+[\\w]+)(\\s+.+|\\s*)$";
	private final static String TO_SEGMENTS = "^\\s*((?i)(?:TO\\s+NAMING)\\s*\\:.+?)((?i)(?:SUBTO\\s+NAMING)\\s*\\:.+|\\s*)";
	private final static String SUBTO_SEGMENTS = "^\\s*((?i)(?:SUBTO\\s+NAMING)\\s*\\:.+?)((?i)(?:SUBTO\\s+NAMING)\\s*\\:.+|\\s*)$";
	private final static String TO_SYNTAX = "^\\s*(?i)(?:TO\\s+NAMING)\\s*\\:\\s*([\\w]+[\\w\\-]+[\\w]+)(\\s+.+|\\s*)$";
	private final static String SUBTO_SYNTAX = "^\\s*(?i)(?:SUBTO\\s+NAMING)\\s*\\:\\s*([\\w]+[\\w\\-]+[\\w]+)(\\s+.+|\\s*)$";
	private final static String BALANCE_SYNTAX = "^\\s*(?i)(?:BALANCE\\s+NAMING)\\s*\\:\\s*([\\w]+[\\w\\-]+[\\w]+)(\\s+.+|\\s*)$";
	private final static String COLLECT_SYNTAX = "^\\s*(?i)(?:COLLECT\\s+NAMING)\\s*\\:\\s*([\\w]+[\\w\\-]+[\\w]+)(\\s+.+|\\s*)$";

	/** 系统参数 **/
	private final static String SITES = "^\\s*(?i)SITES\\s*:\\s*(\\d+)(\\s*\\,.+|\\s*)$";
	private final static String WRITETO = "^\\s*(?i)(?:WRITETO)\\s*\\:\\s*\\\"(.+?)\\\"(\\s*\\,.+|\\s*)$";
	/** FROM命名中的SELECT查询: SELECT * FROM * WHERE * (‘*’号三部分，不可多不可少) **/
	private final static String QUERY = "^\\s*(?i)QUERY\\s*:\\s*\\\"(.+?)\\\"(\\s*\\,.+|\\s*)$";

	/** 自定义参数 **/
	private final static String FILTE_PARAM = "^\\s*(?:,)\\s*(.+)$";
	private final static String CHECK_PARAM = "^\\s*([\\w]+[\\w\\-]*[\\w]*)\\s*\\(\\s*(?i)(RAW|BINARY|BOOLEAN|BOOL|CHAR|STRING|DATE|TIME|DATETIME|TIMESTAMP|SMALLINT|SHORT|INT|LONG|BIGINT|FLOAT|REAL|DOUBLE)\\s*\\)\\s*=(.+)$";
	private final static String PARAM_BOOLEAN = "^\\s*([\\w]+[\\w\\-]*[\\w]*)\\s*\\(\\s*(?i)(?:BOOLEAN|BOOL)\\s*\\)\\s*=\\s*(?i)(TRUE|FALSE)(\\s*\\,.+|\\s*)$";
	private final static String PARAM_CONSTS = "^\\s*([\\w]+[\\w\\-]*[\\w]*)\\s*\\(\\s*(?i)(SMALLINT|SHORT|INT|BIGINT|LONG)\\s*\\)\\s*=\\s*([-]{0,1}[0-9]+)(\\s*\\,.+|\\s*)$";
	private final static String PARAM_FLOAT = "^\\s*([\\w]+[\\w\\-]*[\\w]*)\\s*\\(\\s*(?i)(FLOAT|REAL|DOUBLE)\\s*\\)\\s*=\\s*([-]{0,1}[0-9]{1,}[.]{0,1}[0-9]{1,})(\\s*\\,.+|\\s*)$";
	private final static String PARAM_RAW = "^\\s*([\\w]+[\\w\\-]*[\\w]*)\\s*\\(\\s*(?i)(?:RAW|BINARY)\\s*\\)\\s*=\\s*(?i)0X([0-9a-fA-F]+)(\\s*\\,.+|\\s*)$";
	private final static String PARAM_STRING = "^\\s*([\\w]+[\\w\\-]*[\\w]*)\\s*\\(\\s*(?i)(CHAR|STRING|DATE|TIME|DATETIME|TIMESTAMP)\\s*\\)\\s*=\\s*\\'(.+?)\\'(\\s*\\,.+|\\s*)$";

	/**
	 * 
	 */
	public ConductParser() {
		super();
	}

	/**
	 * 过滤逗号分隔符
	 * 
	 * @param sql
	 * @return
	 */
	private String filtePrefix(String sql) {
		Pattern pattern = Pattern.compile(ConductParser.FILTE_PARAM);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			return matcher.group(1);
		}
		return sql;
	}
	
	/**
	 * 判断是不是自定义值
	 * @param sql
	 * @return
	 */
	private boolean isSelfParameter(String sql) {
		Pattern pattern = Pattern.compile(ConductParser.CHECK_PARAM);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	/**
	 * 解析自定义参数
	 * 参数格式: "name(type)=value1, name(type)='value2', name(type)=0xvalue3"
	 * @param sql
	 * @return
	 */
	private String splitParameter(NamingObject object, String sql) {
		// 解析布尔参数
		Pattern pattern = Pattern.compile(ConductParser.PARAM_BOOLEAN);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String name = matcher.group(1);
			String value = matcher.group(2);
			object.addValue(new CBool(name, "TRUE".equalsIgnoreCase(value)));
			return matcher.group(3);
		}
		// 解析整型值
		pattern = Pattern.compile(ConductParser.PARAM_CONSTS);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String name = matcher.group(1);
			String type = matcher.group(2);
			String value = matcher.group(3);
			if ("SHORT".equalsIgnoreCase(type) || "SMALLINT".equalsIgnoreCase(type)) {
				object.addValue(new CShort(name, java.lang.Short.parseShort(value)));
			} else if ("INT".equalsIgnoreCase(type)) {
				object.addValue(new CInteger(name, java.lang.Integer.parseInt(value)));
			} else if ("BIGINT".equalsIgnoreCase(type) || "LONG".equalsIgnoreCase(type)) {
				object.addValue(new CLong(name, java.lang.Long.parseLong(value)));
			}
			return matcher.group(4);
		}
		// 解析浮点值
		pattern = Pattern.compile(ConductParser.PARAM_FLOAT);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String name = matcher.group(1);
			String type = matcher.group(2);
			String value = matcher.group(3);
			if ("FLOAT".equalsIgnoreCase(type) || "REAL".equalsIgnoreCase(type)) {
				object.addValue(new CFloat(name, java.lang.Float.parseFloat(value)));
			} else if ("DOUBLE".equalsIgnoreCase(type) ) {
				object.addValue(new CDouble(name, java.lang.Double.parseDouble(value)));
			}
			return matcher.group(4);
		}
		// 解析二进制变量值
		pattern = Pattern.compile(ConductParser.PARAM_RAW);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String name = matcher.group(1);
			String value = matcher.group(2);
			byte[] b = super.atob(value);
			object.addValue(new CRaw(name, b));	
			return matcher.group(3);
		}
		// 解析字符串值(字符，日期，时间，时间戳)
		pattern = Pattern.compile(ConductParser.PARAM_STRING);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String name = matcher.group(1);
			String type = matcher.group(2);
			String value = matcher.group(3);
			
			if ("CHAR".equalsIgnoreCase(type) || "STRING".equalsIgnoreCase(type)) {
				object.addValue(new CString(name, value));
			} else if ("DATE".equalsIgnoreCase(type) ) {
				int date = super.splitDate(value);
				object.addValue(new CDate(name, date));
			} else if("TIME".equalsIgnoreCase(type)) {
				int time = super.splitTime(value);
				object.addValue(new CTime(name, time));
			} else if("DATETIME".equalsIgnoreCase(type) || "TIMESTAMP".equalsIgnoreCase(type)) {
				long timestamp = super.splitTimestamp(value);
				object.addValue(new CTimestamp(name, timestamp));
			}
			return matcher.group(4);
		}
		// this is error
		throw new SQLSyntaxException("invalid parameter:%s", sql); 
	}

	/**
	 * 解析"INIT NAMING"
	 * @param sql
	 * @return
	 */
	private InitObject splitInit(String sql) {
		Pattern pattern = Pattern.compile(ConductParser.INIT_SYNTAX);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid init naming:%s", sql);
		}
		// 解析
		String naming = matcher.group(1);
		String suffix = matcher.group(2);

		// 初始化对象
		InitObject object = new InitObject();
		object.setNaming(naming);
		// 解析参数
		while (suffix.trim().length() > 0) {
			// 过滤逗号分隔符
			suffix = this.filtePrefix(suffix);
			
			// 如果是自定义参数
			if(isSelfParameter(suffix)) {
				suffix = this.splitParameter(object, suffix);
				continue;
			}
			
			throw new SQLSyntaxException("cannot resolve: %s", suffix);			
		}
		return object;
	}
	
	/**
	 * 解析"from naming"语句
	 * 
	 * @param sql
	 * @param chooser
	 * @return
	 */
	private FromObject splitFrom(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(ConductParser.FROM_SYNTAX);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("invalid from naming:%s", sql);
		}
		
		// 取出命名和后续参数
		String naming = matcher.group(1);
		String suffix = matcher.group(2);

		FromInputObject object = new FromInputObject();
		
		while(suffix.trim().length() > 0) {
			// 过滤逗号分隔符
			suffix = this.filtePrefix(suffix);
			// 指定主机数
			pattern = Pattern.compile(ConductParser.SITES);
			matcher = pattern.matcher(suffix);
			if (matcher.matches()) {
				int sites = Integer.parseInt(matcher.group(1));
				object.setSites(sites);
				suffix = matcher.group(2);
				continue;
			}
			// SELECT语句
			pattern = Pattern.compile(ConductParser.QUERY);
			matcher = pattern.matcher(suffix);
			if (matcher.matches()) {
				String query = matcher.group(1);
				suffix = matcher.group(2);

				SelectParser parser = new SelectParser();
				Select select = parser.split(query, chooser);
				object.addSelect(select);
				continue;
			}
			// 自定义参数
			if(isSelfParameter(suffix)) {
				suffix = this.splitParameter(object, suffix);
				continue;
			}
			
			throw new SQLSyntaxException("cannot resolve:%s", suffix);
		}

		FromObject from = new FromObject(naming);;
		from.setInput(object);
		return from;
	}
	
	/**
	 * 解析数据合并语句: "to naming", "to naming... subto naming..."
	 * 
	 * @param sql
	 * @return
	 */
	private ToObject splitTo(String sql) {
		Pattern pattern = Pattern.compile(ConductParser.TO_SEGMENTS);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid to naming:%s", sql);
		}

		String prefix = matcher.group(1);
		String suffix = matcher.group(2);

		// 主状态
		ToInputObject object = splitTo(ConductParser.TO_SYNTAX, prefix);
		// 解析子命名
		while (suffix.trim().length() > 0) {
			pattern = Pattern.compile(ConductParser.SUBTO_SEGMENTS);
			matcher = pattern.matcher(suffix);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("invalid subto naming:%s", suffix);
			}

			prefix = matcher.group(1);
			suffix = matcher.group(2);

			ToInputObject slave = splitTo(ConductParser.SUBTO_SYNTAX, prefix);
			if (slave.getSites() == 0) {
				slave.setSites(object.getSites());
			}
			object.setLast(slave);
		}
		
		ToObject to = new ToObject();
		to.setNaming(object.getNaming());
		to.setInput(object);
		return to;
	}
	
	/**
	 * 解析"to naming"和"subto naming"
	 * 
	 * @param regex - 正则表达式格式
	 * @param sql - 正则表达式内容
	 * @return
	 */
	private ToInputObject splitTo(String regex, String sql) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("invalid to or subto naming:%s", sql);
		}

		String naming = matcher.group(1);
		String suffix = matcher.group(2);

		ToInputObject object = new ToInputObject(naming);
		// 解析参数
		while (suffix.trim().length() > 0) {
			// 过滤参数分隔符(逗号)
			suffix = this.filtePrefix(suffix);
			// 指定主机数
			pattern = Pattern.compile(ConductParser.SITES);
			matcher = pattern.matcher(suffix);
			if (matcher.matches()) {
				int sites = Integer.parseInt(matcher.group(1));
				object.setSites(sites);
				suffix = matcher.group(2);
				continue;
			}
			// 如果是自定义参数
			if (isSelfParameter(suffix)) {
				suffix = this.splitParameter(object, suffix);
				continue;
			}
			throw new SQLSyntaxException("cannot resolve:%s", suffix);
		}
		return object;
	}
	
	/**
	 * 解析数据收集语句:"collect naming"
	 * 
	 * @param sql
	 * @return
	 */
	private CollectObject splitCollect(String sql) {
		Pattern pattern = Pattern.compile(ConductParser.COLLECT_SYNTAX);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("invalid collect naming:%s", sql);
		}
		
		// 解析
		String naming = matcher.group(1);
		String suffix = matcher.group(2);

		CollectObject object = new CollectObject();
		object.setNaming(naming);
		
		while (suffix.trim().length() > 0) {
			// 过滤参数分隔符(逗号)
			suffix = this.filtePrefix(suffix);
			// 数据写入的本地文件名
			pattern = Pattern.compile(ConductParser.WRITETO);
			matcher = pattern.matcher(suffix);
			if (matcher.matches()) {
				String writeto = matcher.group(1);
				object.setWriteTo(writeto);
				suffix = matcher.group(2);
				continue;
			}
			// 如果是自定义参数
			if (isSelfParameter(suffix)) {
				suffix = this.splitParameter(object, suffix);
				continue;
			}
			throw new SQLSyntaxException("cannot resolve:%s", suffix);
		}
		return object;
	}

	/**
	 * 解析平衡分布语句: "balance naming"
	 * @param sql
	 * @return
	 */
	private BalanceObject splitBalance(String sql) {
		Pattern pattern = Pattern.compile(ConductParser.BALANCE_SYNTAX);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("invalid balance naming:%s", sql);
		}
		
		// 解析
		String naming = matcher.group(1);
		String suffix = matcher.group(2);

		BalanceObject object = new BalanceObject();
		object.setNaming(naming);
		
		while(suffix.trim().length() > 0) {
			// 过滤参数分隔符(逗号)
			suffix = this.filtePrefix(suffix);
			// 如果是自定义参数
			if(isSelfParameter(suffix)) {
				suffix = this.splitParameter(object, suffix);
				continue;
			}
			throw new SQLSyntaxException("cannot resolve:%s", suffix);
		}
		return object;
	}
	
	/**
	 * 解析Conduct命令
	 * 
	 * @param sql
	 * @param tables
	 * @return
	 */
	public Conduct split(String sql, SQLChooser tables) {
		Pattern pattern = Pattern.compile(ConductParser.CONDUCT_PREFIX);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid conduct:%s", sql);
		}

		String suffix = matcher.group(1);
		Conduct conduct = new Conduct();

		while (suffix.trim().length() > 0) {
			pattern = Pattern.compile(ConductParser.CONDUCT_SEGMENTS);
			matcher = pattern.matcher(suffix);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("invalid conduct entity:%s", suffix);
			}

			String segment = matcher.group(1);
			String naming = matcher.group(2);
			suffix = matcher.group(3);

			if ("INIT".equalsIgnoreCase(naming)) {
				if (conduct.getInit() != null) {
					throw new SQLSyntaxException("init naming duplicate:%s", segment);
				}
				conduct.setInit(this.splitInit(segment));
			} else if ("FROM".equalsIgnoreCase(naming)) {
				if (conduct.getFrom() != null) {
					throw new SQLSyntaxException("from naming duplicate:%s", segment);
				}
				conduct.setFrom(this.splitFrom(segment, tables));
			} else if ("BALANCE".equalsIgnoreCase(naming)) {
				if (conduct.getBalance() != null) {
					throw new SQLSyntaxException("balance naming duplicate:%s", segment);
				}
				conduct.setBalance(this.splitBalance(segment));
			} else if ("TO".equalsIgnoreCase(naming)) {
				if (conduct.getTo() != null) {
					throw new SQLSyntaxException("to naming duplicate:%s", segment);
				}
				conduct.setTo(this.splitTo(segment));
			} else if ("COLLECT".equalsIgnoreCase(naming)) {
				if (conduct.getCollect() != null) {
					throw new SQLSyntaxException("collect naming duplicate:%s", segment);
				}
				conduct.setCollect(this.splitCollect(segment));
			}
		}
		
		if (conduct.getInit() == null) {
			throw new SQLSyntaxException("cannot define \'init naming\'");
		} else if (conduct.getFrom() == null) {
			throw new SQLSyntaxException("cannot define \'from naming\'");
		} else if (conduct.getTo() == null) {
			throw new SQLSyntaxException("cannot define \'to naming\'");
		}

		return conduct;
	}

}
