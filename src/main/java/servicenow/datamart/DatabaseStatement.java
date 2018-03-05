package servicenow.datamart;

import servicenow.api.*;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Pattern;
import org.slf4j.Logger;

public abstract class DatabaseStatement {

	final Database db;
	final String sqlTableName;
	final Generator generator;
	final ColumnDefinitions columns;
	final String templateName;
	final String stmtText;
	final PreparedStatement stmt;
	Record lastRec;

    private final Pattern dateTimePattern = 
        	Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d");

	public final Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    
	final private Logger logger = Log.logger(this.getClass());
	final boolean traceEnabled;
	
	public DatabaseStatement(Database db, String templateName, String sqlTableName, ColumnDefinitions columns) throws SQLException {
		this.db = db;
		this.sqlTableName = sqlTableName;
		this.templateName = templateName;
		this.generator = db.getGenerator();
		Connection dbc = db.getConnection();
		this.columns = columns;
		this.stmtText = buildStatement();
		logger.debug(Log.SCHEMA, stmtText);
		this.stmt = dbc.prepareStatement(stmtText);
		traceEnabled = logger.isTraceEnabled(Log.BIND);
	}

	abstract String buildStatement() throws SQLException;
		
	protected void bindField(int bindCol, String value) throws SQLException {
		stmt.setString(bindCol, value);
	}
	
	/**
	 * Get a value from a Glide Record and bind it to a variable in prepared statement.
	 * 
	 * @param bindCol Index (starting with 1) of the variable within the statement.
	 * @param rec Glide Record which serves as a source of the data.
	 * @param glideCol Index (starting with 0) of the variable in the columns array.
	 * @throws SQLException
	 */	
	protected void bindField(int bindCol, Record rec, int glideCol) throws SQLException {
		DatabaseFieldDefinition defn = columns.get(glideCol);
		String glidename = defn.getGlideName();
		String value = rec.getValue(glidename);
		try {
			bindField(bindCol, rec, defn, value);
		}
		catch (SQLException|NumberFormatException e) {
			logger.error(Log.PROCESS, 
					String.format("bindField %s=\"%s\"", glidename, value));
			throw e;
		}		
	}
	
	protected void bindField(int bindCol, Record rec, DatabaseFieldDefinition d, String value) throws SQLException {
		String glidename = d.getGlideName();
		int sqltype = d.sqltype;
		// If value is null then bind to null and exit
		if (value == null) {
			stmt.setNull(bindCol, sqltype);
			return;
		}		
		if ((sqltype == Types.NUMERIC || sqltype == Types.DECIMAL || 
				sqltype == Types.INTEGER || sqltype == Types.DOUBLE) && 
				value.length() == 19) {
			// If the target data type is numeric
			// and the value appears to be a date (dddd-dd-dd dd:dd:dd)
			// then it must be a duration
			// so try to convert it to a number of seconds
			if (dateTimePattern.matcher(value).matches()) {
				try {
					DateTime timestamp = new DateTime(value, DateTime.DATE_TIME);
					long seconds = timestamp.toDate().getTime() / 1000L;
					if (traceEnabled)
						logger.trace(Log.BIND, "date " + glidename + " " + value + "=" + seconds);
					if (seconds < 0L) {
						logger.warn(Log.PROCESS, rec.getKey() + " duration underflow: " +
							glidename + "=" + value);
						value = null;
					}
					else if (seconds > 999999999L) {
						logger.warn(Log.PROCESS, rec.getKey() + " duration overflow: " +
							glidename + "=" + value);
						value = null;
					}
					else {
						value = Long.toString(seconds);
					}
				} catch (InvalidDateTimeException e) {
					logger.warn(Log.PROCESS, rec.getKey() + " duration error: " +
							glidename + "=" + value);
					value = null;
				}
				if (value == null) {
					stmt.setNull(bindCol, sqltype);
					return;					
				}
			}
		}
		assert value != null;
		// If the SQL type is VARCHAR, then check for an over-size value
		// and truncate if necessary
		if (sqltype == Types.VARCHAR || sqltype == Types.CHAR) {
			int oldSize = value.length();
			int maxSize = d.getSize();
			if (value.length() > maxSize) {
				value = value.substring(0,  maxSize);
			}
			if (generator.getDialectName().equals("oracle_")) {
				// This is a workaround for an apparent bug in the Oracle JDBC 
				// driver which occasionally generates an ORA-01461 error when 
				// inserting from a text field containing multi-byte characters
				// into a VARCHAR2 column.
				// Keep chopping more characters off the end of the string until
				// the number of BYTES is less than the field size.
				try {
					while (value.getBytes("UTF8").length > maxSize)
						value = value.substring(0, value.length() - 1);										
				}
				catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
			}			
			if (value.length() != oldSize) {
				String message = rec.getKey() + " truncated: " + glidename +
					" from " + oldSize + " to " + value.length();
				if (Globals.warnOnTruncate)
					logger.warn(Log.PROCESS, message);
				else
					logger.debug(Log.PROCESS, message);
			}
		}
		if (traceEnabled) {
			int len = (value == null ? 0 : value.length());
			logger.trace(Log.BIND, String.format("bind %d %s %s=%s (len=%d)",
					bindCol, sqlTypeName(sqltype), glidename, value, len));
		}
		assert value != null;
		switch (sqltype) {
		case Types.DATE :
			DateTime dt;
			try {
				dt = new DateTime(value);
				java.sql.Date sqldate = new java.sql.Date(dt.getMillisec());
				stmt.setDate(bindCol, sqldate, GMT);
			}
			catch (InvalidDateTimeException e) {
				logger.warn(Log.PROCESS, rec.getKey() + " date error: " +
						glidename + "=" + value);
				stmt.setDate(bindCol,  null);			
			}
			break;
		case Types.TIMESTAMP :
			// If the SQL type is TIMESTAMP, then try to bind the field to a java.sql.Timesetamp.
			// Note that in Oracle the DATE fields have a java.sql type of TIMESTAMP.
			DateTime ts;
			try { 
				ts = new DateTime(value);
				java.sql.Timestamp sqlts = new java.sql.Timestamp(ts.getMillisec());
				assert sqlts.getTime() == ts.getMillisec();
				if (traceEnabled)
					logger.trace(Log.BIND, String.format("timestamp %s=%s", glidename, sqlts.toString()));
				stmt.setTimestamp(bindCol, sqlts, GMT);
			}
			catch (InvalidDateTimeException e) {
				logger.warn(Log.PROCESS, rec.getKey() + " timestamp error: " +
						glidename + "=" + value);
				stmt.setTimestamp(bindCol, null);				
			}
			break;
		case Types.BOOLEAN :
		case Types.BIT :
			if (value.equals("1") || value.equalsIgnoreCase("true"))
				stmt.setBoolean(bindCol, true);
			else if (value.equals("0") || value.equalsIgnoreCase("false"))
				stmt.setBoolean(bindCol,  false);
			else {
				logger.warn(Log.PROCESS, rec.getKey() + "boolean error: " +
						glidename + "=" + value);
				stmt.setNull(bindCol, sqltype);
			}
			break;
		case Types.TINYINT :
			if (value.equalsIgnoreCase("false")) value = "0";
			if (value.equalsIgnoreCase("true"))  value = "1";
			stmt.setByte(bindCol, Byte.parseByte(value));
			break;
		case Types.SMALLINT :
			if (value.equalsIgnoreCase("false")) value = "0";
			if (value.equalsIgnoreCase("true"))  value = "1";
			stmt.setShort(bindCol, Short.parseShort(value));
			break;
		case Types.INTEGER :
			// This is a workaround for the fact that ServiceNow includes decimal portions
			// in integer fields, which can cause JDBC to choke.
			int p = value.indexOf('.');
			if (p > -1) {
				String message = rec.getKey() + " decimal truncated: " +
						glidename + "=" + value;
				if (Globals.warnOnTruncate)
					logger.warn(Log.PROCESS, message);
				else
					logger.debug(Log.PROCESS, message);
				value = value.substring(0,  p);
			}
			if (value.length() == 0) value = "0";
			if (value.equalsIgnoreCase("false")) value = "0";
			if (value.equalsIgnoreCase("true"))  value = "1";
			stmt.setInt(bindCol, Integer.parseInt(value));
			break;
		case Types.DOUBLE :
		case Types.FLOAT :
		case Types.NUMERIC :
		case Types.DECIMAL :
			if (value.equalsIgnoreCase("false")) value = "0";
			if (value.equalsIgnoreCase("true"))  value = "1";
			stmt.setDouble(bindCol, Double.parseDouble(value));
			break;
		default :
			stmt.setString(bindCol, value);			
		}
	}

	private static String sqlTypeName(int sqltype) {
		switch (sqltype) {
			case Types.ARRAY:         return "ARRAY";
			case Types.BIGINT:        return "BIGINT";
			case Types.BINARY:        return "BINARY";
			case Types.BIT:           return "BIT";
			case Types.BLOB:          return "BLOB";
			case Types.BOOLEAN:       return "BOOLEAN";
			case Types.CHAR:          return "CHAR";
			case Types.CLOB:          return "CLOB";
			case Types.DATALINK:      return "DATALINK";
			case Types.DATE:          return "DATE";
			case Types.DECIMAL:       return "DECIMAL";
			case Types.DISTINCT:      return "DISTINCT";
			case Types.DOUBLE:        return "DOUBLE";
			case Types.FLOAT:         return "FLOAT";
			case Types.INTEGER:       return "INTEGER";
			case Types.JAVA_OBJECT:   return "JAVA_OBJECT";
			case Types.LONGNVARCHAR:  return "LONGNVARCHAR";
			case Types.LONGVARBINARY: return "LONGVARBINARY";
			case Types.LONGVARCHAR:   return "LONGVARCHAR";
			case Types.NCHAR:         return "NCHAR";
			case Types.NCLOB:         return "NCLOB";
			case Types.NULL:          return "NULL";
			case Types.NUMERIC:       return "NUMERIC";
			case Types.NVARCHAR:      return "NVARCHAR";
			case Types.OTHER:         return "OTHER";
			case Types.REAL:          return "REAL";
			case Types.REF:           return "REF";
			case Types.ROWID:         return "ROWID";
			case Types.SMALLINT:      return "SMALLINT";
			case Types.SQLXML:        return "SQLXML";
			case Types.STRUCT:        return "STRUCT";
			case Types.TIME:          return "TIME";
			case Types.TIMESTAMP:     return "TIMESTAMP";
			case Types.TINYINT:       return "TINYINT";
			case Types.VARBINARY:     return "VARBINARY";
			case Types.VARCHAR:       return "VARCHAR";
			default: return Integer.toString(sqltype);
		}
	}
}
