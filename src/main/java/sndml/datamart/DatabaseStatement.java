package sndml.datamart;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.regex.Pattern;
import org.slf4j.Logger;

import sndml.servicenow.DateTime;
import sndml.servicenow.InvalidDateTimeException;
import sndml.servicenow.Log;
import sndml.servicenow.TableRecord;
import sndml.servicenow.RecordKey;

/**
 * This abstract class has three derived classes:
 * <ul>
 * <li>{@link DatabaseInsertStatement}</li>
 * <li>{@link DatabaseUpdateStatement}</li>
 * <li>{@link DatabaseDeleteStatement}</li>
 * </ul>
 *
 */
public abstract class DatabaseStatement {

	final Database db;
	final Calendar calendar;
	final String sqlTableName;
	final Generator generator;
	final ColumnDefinitions columns;
	final String templateName;
	final String stmtText;
	final PreparedStatement stmt;
	TableRecord rec;

    private final Pattern dateTimePattern = 
        	Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d");

    
	final protected Logger logger = Log.logger(this.getClass());
	final boolean traceEnabled;
	
	public DatabaseStatement(Database db, String templateName, String sqlTableName, ColumnDefinitions columns) throws SQLException {
		this.db = db;
		this.calendar = db.getCalendar();
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
		
	protected void setRecord(TableRecord rec) {
		this.rec = rec;
	}
		
	/**
	 * Bind a string value to a variable in a prepared statement.
	 * Called from {@link DatabaseDeleteStatement}
	 * 
	 * @param bindCol Index (starting with 1) of the variable within the statement.
	 * @param value Value to be bound
	 * @throws SQLException
	 */
	protected void bindField(int bindCol, String value) throws SQLException {
		stmt.setString(bindCol, value);
	}
	
	/**
	 * Get a value from a Glide Record and bind it to a variable in prepared statement.
	 * 
	 * @param bindCol Index (starting with 1) of the variable within the statement.
	 * @param glideCol Index (starting with 0) of the variable in the columns array.
	 * @throws SQLException
	 */	
	protected void bindField(int bindCol, int glideCol) throws SQLException {
		assert this.rec != null;
		DatabaseFieldDefinition defn = columns.get(glideCol);
		String fieldname = defn.getGlideName();
		String value = rec.getValue(fieldname);
		try {
			bindField(bindCol, defn, fieldname, value);
		}
		catch (SQLException|NumberFormatException e) {
			fieldname = defn.getGlideName();
			RecordKey key = rec.getKey();
			String typename = sqlTypeName(defn.sqltype);
			logger.error(Log.PROCESS, 
					String.format(
						"bindField sys_id=%s field=%s type=%s value=\"%s\"", 
						key, fieldname, typename, value));
			logger.error(Log.PROCESS, rec.asText(true));
			throw e;
		}		
	}
	
	/**
	 * Bind a value to a variable in a prepared statement
	 * 
	 * @param bindCol Index (starting with 1) of the variable within the statement
	 * @param defn Database field definition
	 * @param fieldname Name of the field
	 * @param value Value to be bound
	 * @throws SQLException
	 */
	protected void bindField(int bindCol, DatabaseFieldDefinition defn, String fieldname, String value) 
			throws SQLException {
		int sqltype = defn.sqltype;
		// If value is null then bind to null and exit
		if (value == null) {
			stmt.setNull(bindCol, sqltype);
			return;
		}		
		if ((sqltype == Types.NUMERIC || sqltype == Types.DECIMAL || sqltype == Types.INTEGER || 
					sqltype == Types.DOUBLE || sqltype == Types.BIGINT) && 
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
						logger.trace(Log.BIND, "date " + fieldname + " " + value + "=" + seconds);
					if (seconds < 0L) {
						logger.warn(Log.PROCESS, rec.getKey() + " duration underflow: " +
							fieldname + "=" + value);
						value = null;
					}
					if (seconds > 999999999L && sqltype == Types.INTEGER) {
						logger.warn(Log.PROCESS, rec.getKey() + " duration overflow: " +
							fieldname + "=" + value);
						value = null;
					}
					if (value != null) {
						value = Long.toString(seconds);
					}
				} catch (InvalidDateTimeException e) {
					logger.warn(Log.PROCESS, rec.getKey() + " duration error: " +
							fieldname + "=" + value);
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
			int maxSize = defn.getSize();
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
			if (db.isPostgreSQL()) {
				// PostgreSQL doesn't support storing NULL characters in text fields
				value = value.replaceAll("\u0000", "");
			}
			if (value.length() != oldSize) {
				String message = rec.getKey() + " truncated: " + fieldname +
					" from " + oldSize + " to " + value.length();
				if (db.getWarnOnTruncate())
					logger.warn(Log.PROCESS, message);
				else
					logger.debug(Log.PROCESS, message);
			}
		}
		if (traceEnabled) {
			int len = (value == null ? 0 : value.length());
			logger.trace(Log.BIND, String.format("bind %d %s %s=%s (len=%d)",
					bindCol, sqlTypeName(sqltype), fieldname, value, len));
		}
		assert value != null;
		switch (sqltype) {
		case Types.DATE :
			DateTime dt;
			try {
				dt = new DateTime(value);
				java.sql.Date sqldate = new java.sql.Date(dt.getMillisec());
				stmt.setDate(bindCol, sqldate, calendar);
			}
			catch (InvalidDateTimeException e) {
				logger.warn(Log.PROCESS, rec.getKey() + " date error: " +
						fieldname + "=" + value);
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
					logger.trace(Log.BIND, String.format("timestamp %s=%s", fieldname, sqlts.toString()));
				stmt.setTimestamp(bindCol, sqlts, calendar);
			}
			catch (InvalidDateTimeException e) {
				logger.warn(Log.PROCESS, rec.getKey() + " timestamp error: " +
						fieldname + "=" + value);
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
						fieldname + "=" + value);
				stmt.setNull(bindCol, sqltype);
			}
			break;
		case Types.TINYINT :
			value = truncate(fieldname, value);
			stmt.setByte(bindCol, Byte.parseByte(value));
			break;
		case Types.SMALLINT :
			value = truncate(fieldname, value);
			stmt.setShort(bindCol, Short.parseShort(value));
			break;			
		case Types.INTEGER :
			value = truncate(fieldname, value);
			stmt.setInt(bindCol, Integer.parseInt(value));
			break;
		case Types.BIGINT:
			stmt.setLong(bindCol, Long.parseLong(value));
			break;
		case Types.DOUBLE :
		case Types.FLOAT :
		case Types.NUMERIC :
		case Types.DECIMAL :
			stmt.setDouble(bindCol, Double.parseDouble(value));
			break;
		default :
			if (db.isPostgreSQL()) {
				// PostgreSQL doesn't support storing NULL characters in text fields
				value = value.replaceAll("\u0000", "");
			}
			stmt.setString(bindCol, value);			
		}
	}

	// Return a string that will not throw an error when parseInt is called
	private String truncate(String fieldname, String value) {
		if (value.length() == 0) return "0";
		// This is a workaround for the fact that ServiceNow includes decimal portions
		// in integer fields, which can cause JDBC to choke.
		int p = value.indexOf('.');
		if (p > -1) {
			String message = rec.getKey() + " decimal truncated: " +
					fieldname + "=" + value;
			if (db.getWarnOnTruncate())
				logger.warn(Log.PROCESS, message);
			else
				logger.debug(Log.PROCESS, message);
			return value.substring(0,  p);
		}
		return value;
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
