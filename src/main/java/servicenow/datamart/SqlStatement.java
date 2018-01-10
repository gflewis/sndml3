package servicenow.datamart;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import servicenow.core.*;

public abstract class SqlStatement {

	final Database dbw;
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
	
	public SqlStatement(Database dbw, String templateName, String sqlTableName, ColumnDefinitions columns) throws SQLException {
		this.dbw = dbw;
		this.sqlTableName = sqlTableName;
		this.templateName = templateName;
		this.generator = dbw.getGenerator();
		Connection dbc = dbw.getConnection();
		this.columns = columns;
		this.stmtText = buildStatement();
		logger.debug(Log.INIT, stmtText);
		this.stmt = dbc.prepareStatement(stmtText);		
	}

	abstract String buildStatement() throws SQLException;
		
	/**
	 * Get a value from a Glide Record and bind it to a variable in prepared statement.
	 * 
	 * @param bindCol Index (starting with 1) of the variable within the statement.
	 * @param rec Glide Record which serves as a source of the data.
	 * @param glideCol Index (starting with 0) of the variable in the columns array.
	 * @throws SQLException
	 */	
	public void bindField(int bindCol, Record rec, int glideCol) throws SQLException {
		SqlFieldDefinition defn = columns.get(glideCol);
		String glidename = defn.getGlideName();
		String value = rec.getValue(glidename);
		bindField(bindCol, rec, defn, value);		
	}
	
	public void bindField(int bindCol, Record rec, SqlFieldDefinition d, String value) throws SQLException {
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
					if (logger.isTraceEnabled())
						logger.trace(Log.PROCESS, glidename + " " + value + "=" + seconds);
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
		if (logger.isTraceEnabled()) {
			int len = (value == null ? 0 : value.length());
			logger.trace(Log.PROCESS, String.format("bind (%d:%d) %s=%s [%d]",
					bindCol, sqltype, glidename, value, len));
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
			stmt.setByte(bindCol, Byte.parseByte(value));
			break;
		case Types.SMALLINT :
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
			if (value == "") value = "0";
			stmt.setInt(bindCol, Integer.parseInt(value));
			break;
		case Types.DOUBLE :
		case Types.FLOAT :
		case Types.NUMERIC :
		case Types.DECIMAL :
			stmt.setDouble(bindCol, Double.parseDouble(value));
			break;
		default :
			stmt.setString(bindCol, value);
		}
	}
	
}
