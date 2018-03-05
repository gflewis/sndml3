package servicenow.datamart;

import servicenow.api.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseTimestampReader {

	final Database database;
	final Connection dbc;
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	DatabaseTimestampReader(Database database) {
		this.database = database;
		this.dbc = database.getConnection();
	}
	
	DateTime getTimestampUpdated(String tableName, Key key) throws SQLException {
		String sql = database.getGenerator().getTemplate("rec_timestamps", tableName, null);
		DateTime result = null;
		PreparedStatement stmt = dbc.prepareStatement(sql);
		stmt.setString(1,  key.toString());
		ResultSet rs = stmt.executeQuery();
		if (rs.next() ) {
			Timestamp sys_updated_on = rs.getTimestamp(1);
			result = new DateTime(sys_updated_on);
			logger.debug(Log.TEST, String.format(
					"%s updated=%s (%s)", key.toString(), sys_updated_on.toString(), result));
		}
		rs.close();
		return result;
	}

	DateTime getTimestampCreated(String tableName, Key key) throws SQLException {
		String sql = database.getGenerator().getTemplate("rec_timestamps", tableName, null);
		DateTime result = null;
		PreparedStatement stmt = dbc.prepareStatement(sql);
		stmt.setString(1,  key.toString());
		ResultSet rs = stmt.executeQuery();
		if (rs.next() ) {
			Timestamp sys_created_on = rs.getTimestamp(2);			
			result = new DateTime(sys_created_on);
			logger.debug(Log.TEST, String.format(
				"%s created=%s (%s)", key.toString(), sys_created_on.toString(), result));
		}
		rs.close();
		return result;
	}
	
	TimestampLookup getTimestamps(String tableName) throws SQLException {
		String stmtText = database.getGenerator().getTemplate("all_timestamps", tableName, null);
		return getQueryResult(stmtText);
	}
	
	TimestampLookup getTimestamps(String tableName, DateTimeRange created) throws SQLException {
		Parameters vars = new Parameters();
		vars.put("start", created.getStart().toString());
		vars.put("end",  created.getEnd().toString());
		String stmtText = database.getGenerator().getTemplate("patition_timestamps", tableName, vars);
		return getQueryResult(stmtText);
	}
	
	private TimestampLookup getQueryResult(String stmtText) throws SQLException {
		TimestampLookup result = new TimestampLookup();
		PreparedStatement stmt = dbc.prepareStatement(stmtText);
		ResultSet rs = stmt.executeQuery();
		while (rs.next() ) {
			String sys_id = rs.getString(1);
			java.util.Date sys_updated_on = rs.getDate(2);
			Key key = new Key(sys_id);
			DateTime value = new DateTime(sys_updated_on);
			result.put(key, value);
		}
		rs.close();
		return result;		
		
	}
}
