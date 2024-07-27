package sndml.loader;

import sndml.util.Log;

import static org.junit.Assert.*;

import org.slf4j.Logger;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtil {
		
	DatabaseWrapper dbWrapper;	
	Logger logger = TestManager.getLogger(DBUtil.class);

	DBUtil(DatabaseWrapper dbWrapper) {
		this.dbWrapper = dbWrapper;		
	}
	
	DBUtil(Resources resources) {
		this.dbWrapper = resources.getDatabaseWrapper();
	}
	
	DBUtil(TestingProfile profile) throws SQLException, URISyntaxException {
		this(new Resources(profile));
	}
	
	DBUtil() throws SQLException, URISyntaxException {
		this(TestManager.getProfile());
	}
	
	DatabaseWrapper getDatabase() {
		return this.dbWrapper;
	}
	
	/**
	 * Execute an SQL statement
	 */
	int sqlUpdate(String sql) throws SQLException {
		Connection dbc = dbWrapper.getConnection();
		assertNotNull(dbc);
		logger.debug(Log.TEST, "sqlUpdate \"" + sql + "\"");
		Statement stmt = dbc.createStatement();
		int count = 0;
		try {
			count = stmt.executeUpdate(sql);
			dbWrapper.commit();
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
		dbWrapper.truncateTable(tablename);		
		logger.info(Log.TEST, "end truncateTable " + tablename);
	}
	
	/**
	 * Drop a table if it exists
	 */
	void dropTable(String tablename) throws SQLException {
		logger.info(Log.TEST, "begin dropTable " + tablename);
		dbWrapper.dropTable(tablename, true);
		logger.info(Log.TEST, "end dropTable " + tablename);
	}
	
	boolean tableExists(String tablename) throws SQLException {
		return dbWrapper.tableExists(tablename);
	}
	
	void commit() throws SQLException {
		dbWrapper.commit();
	}
	

	int sqlCount(String tablename, String where) throws SQLException {
		String query = "select count(*) from " + dbWrapper.qualifiedName(tablename);
		if (where != null) query += " where " + where;
		return sqlCount(query);
	}
	
	int sqlCount(String query) throws SQLException {		
		dbWrapper.commit();
		Statement stmt = dbWrapper.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		int count = rs.getInt(1);
		rs.close();
		dbWrapper.commit();
		logger.info(query + " (" + count + ")");
		return count;
	}

	
	ResultSet getRow(String query) throws SQLException {
		dbWrapper.commit();
		Statement stmt = dbWrapper.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		return rs;		
	}
	
}
