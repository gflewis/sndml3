package sndml.datamart;

import sndml.servicenow.*;

import static org.junit.Assert.*;

import org.slf4j.Logger;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtil {
		
	Database db;
	Logger logger = TestManager.getLogger(DBUtil.class);

	DBUtil(Database database) {
		this.db = database;
	}
	
	DBUtil(TestingProfile profile) throws SQLException, URISyntaxException {
		this(profile.getDatabase());
	}
	
	DBUtil() throws SQLException, URISyntaxException {
		this(TestManager.getProfile());
	}
	
	Database getDatabase() {
		return this.db;
	}
	
	/**
	 * Execute an SQL statement
	 */
	int sqlUpdate(String sql) throws SQLException {
		Connection dbc = db.getConnection();
		assertNotNull(dbc);
		logger.debug(Log.TEST, "sqlUpdate \"" + sql + "\"");
		Statement stmt = dbc.createStatement();
		int count = 0;
		try {
			count = stmt.executeUpdate(sql);
			db.commit();
		} catch (SQLException e) {
			logger.error(Log.TEST, sql, e);
			throw e;
		}
		logger.info(Log.TEST, sql + " (" + count + ")");
		return count;
	}

	/**
	 * Delete all records in a table
	 */
	void truncateTable(String tablename) throws SQLException {		
		logger.info(Log.TEST, "begin truncateTable " + tablename);
		db.truncateTable(tablename);		
		logger.info(Log.TEST, "end truncateTable " + tablename);
	}
	
	/**
	 * Drop a table if it exists
	 */
	void dropTable(String tablename) throws SQLException {
		logger.info(Log.TEST, "begin dropTable " + tablename);
		db.dropTable(tablename, true);
		logger.info(Log.TEST, "end dropTable " + tablename);
	}
	
	boolean tableExists(String tablename) throws SQLException {
		return db.tableExists(tablename);
	}
	
	void commit() throws SQLException {
		db.commit();
	}
	

	int sqlCount(String tablename, String where) throws SQLException {
		String query = "select count(*) from " + tablename;
		if (where != null) query += " where " + where;
		return sqlCount(query);
	}
	
	int sqlCount(String query) throws SQLException {		
		db.commit();
		Statement stmt = db.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		int count = rs.getInt(1);
		rs.close();
		db.commit();
		logger.info(query + " (" + count + ")");
		return count;
	}

	
	ResultSet getRow(String query) throws SQLException {
		db.commit();
		Statement stmt = db.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		return rs;		
	}
	
}
