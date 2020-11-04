package ozgurdbsync;

import java.sql.Types;

public class ColumnInfo {
	private boolean autoIncrement;
	private boolean nullable;
	private int index;
	private boolean isToQuote;

	public ColumnInfo(int index, String name, int type, boolean autoIncrement, boolean nullable) {
		this.index=index;
		this.name = name;
		this.type = type;
		this.autoIncrement = autoIncrement;
		this.nullable = nullable;
		this.isToQuote=isToQuoteInSql();
	}

	String name;
	int type;

	public String toString() {
		return type + ":" + name;
	}
	
	public String toSqlValue(Object o) {
		if(o==null)
			return "null";
		if(isToQuote)
			return singleQuotedString(o.toString());
		return o.toString();
	}
	
	private boolean isToQuoteInSql() {
		switch(type) {
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
		case Types.FLOAT:
		case Types.REAL:
		case Types.DOUBLE:
		case Types.NUMERIC:
		case Types.DECIMAL:
		case Types.NULL:
		case Types.BOOLEAN:
		case Types.ROWID:
			return false;
		}
		return true;
	}
	 public  String singleQuotedString(String str) {
	        StringBuilder result = new StringBuilder("E'");
	        for (int i = 0; i < str.length(); i++) {
	            char ch = str.charAt(i);
	            if (ch == '\n') {
	                result.append("\\n");
	                continue;
	            }/*from w  ww .  j  av  a2 s . c o  m*/
	            if (ch == '\r') {
	                result.append("\\r");
	                continue;
	            }
	            if (ch == '\t') {
	                result.append("\\t");
	                continue;
	            }
	            if (ch == '\'' || ch == '\\')
	                result.append('\\');
	            result.append(ch);
	        }
	        result.append("'");
	        return result.toString();
	    }
}