package sndml.loader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;
import sndml.util.DateTime;
import sndml.util.DateTimeRange;
import sndml.util.Log;

/**
 * Class to read values of sys_id, sys_created_on and sys_updated_on 
 * from an SQL table.
 *
 */
public class DatabaseTimestampReader {

	final DatabaseWrapper database;
	final Connection dbc;
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	final Calendar tzGMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	
	public DatabaseTimestampReader(DatabaseWrapper database) {
		this.database = database;
		this.dbc = database.getConnection();
	}
	
	/** 
	 * Returns sys_updated_on value for a specific record or null if record not found
	 */
	public DateTime getTimestampUpdated(String tableName, RecordKey key) throws SQLException {
		assert tableName != null;
		assert key != null;
		DateTime result = null;
		Generator generator = database.getGenerator();
		String stmtText = generator.getTemplate("select_updated", tableName);
		stmtText += " WHERE " + generator.sqlName("sys_id") + " = ?";
		PreparedStatement stmt = dbc.prepareStatement(stmtText);
		stmt.setString(1, key.toString());
		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			Timestamp sys_updated_on = rs.getTimestamp(2, tzGMT);
			long time = sys_updated_on.getTime();
			result = new DateTime(sys_updated_on);
			logger.debug(Log.TEST, String.format(
					"%s updated=%s time=%d result=%s", key.toString(), sys_updated_on.toString(), time, result));
		}
		rs.close();
		return result;
	}

	/** 
	 * Returns sys_created_on value for a specific record or null if record not found
	 */
	public DateTime getTimestampCreated(String tableName, RecordKey key) throws SQLException {
		assert tableName != null;
		assert key != null;
		DateTime result = null;
		Generator generator = database.getGenerator();
		String stmtText = generator.getTemplate("select_created", tableName);
		stmtText += " WHERE " + generator.sqlName("sys_id") + " = ?";
		PreparedStatement stmt = dbc.prepareStatement(stmtText);
		stmt.setString(1, key.toString());
		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			Timestamp sys_created_on = rs.getTimestamp(2, tzGMT);
			long time = sys_created_on.getTime();
			result = new DateTime(sys_created_on);
			logger.debug(Log.TEST, String.format(
				"%s created=%s time=%d result=%s", key.toString(), sys_created_on.toString(), time, result));
		}
		rs.close();
		return result;
	}
	
	public TimestampHash getTimestamps(String tableName) throws SQLException {
		assert tableName != null;
		Generator generator = database.getGenerator();
		String stmtText = generator.getTemplate("select_updated", tableName);
		PreparedStatement stmt = dbc.prepareStatement(stmtText);
		return getQueryResult(stmt);
	}
	
	public TimestampHash getTimestamps(String tableName, DateTimeRange created) throws SQLException {
		assert tableName != null;
		Generator generator = database.getGenerator();
		String stmtText = generator.getTemplate("select_updated", tableName);
		boolean hasStart = created != null && created.hasStart();
		boolean hasEnd   = created != null && created.hasEnd();
		if (hasStart || hasEnd) {
			stmtText += " WHERE ";
			String sys_created_name = generator.sqlName("sys_created_on");
			if (hasStart) stmtText += sys_created_name + " >= ?";
			if (hasStart && hasEnd) stmtText += " AND ";
			if (hasEnd) stmtText += sys_created_name + " < ?";
		}
		logger.debug(Log.INIT, stmtText);
		PreparedStatement stmt = dbc.prepareStatement(stmtText);
		int bind = 0;
		if (hasStart) stmt.setTimestamp(++bind, created.getStart().toTimestamp());
		if (hasEnd)   stmt.setTimestamp(++bind, created.getEnd().toTimestamp());
		return getQueryResult(stmt);
	}
	
	private TimestampHash getQueryResult(PreparedStatement stmt) throws SQLException {
		TimestampHash result = new TimestampHash();
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			String sys_id = rs.getString(1);
			Timestamp sys_updated_on = rs.getTimestamp(2, tzGMT);
			RecordKey key = new RecordKey(sys_id);
			DateTime value = new DateTime(sys_updated_on);
			result.put(key, value);
		}
		rs.close();
		return result;	
	}
}
