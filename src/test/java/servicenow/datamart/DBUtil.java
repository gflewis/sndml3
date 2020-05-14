package servicenow.datamart;

import servicenow.api.*;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtil {
		
	static Logger logger = TestingManager.getLogger(DBUtil.class);
		
	static void initialize() throws TestingException, SQLException {
		Database db = ResourceManager.getDatabase();
		assert db != null;
		assert db.getConnection() != null;
	}

	/**
	 * Execute an SQL statement
	 */
	static int sqlUpdate(String sql) throws SQLException {
		Database db = ResourceManager.getDatabase();
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
	static void truncateTable(String tablename) throws SQLException {
		Database db = ResourceManager.getDatabase();
		db.truncateTable(tablename);		
	}
	
	/**
	 * Drop a table if it exists
	 */
	static void dropTable(String tablename) throws SQLException {
		Database db = ResourceManager.getDatabase();
		logger.debug(Log.TEST, "dropTable " + tablename);			
		Connection dbc = db.getConnection();
		assertNotNull(dbc);
		Statement stmt = dbc.createStatement();
		String sql = "drop table " + db.qualifiedName(tablename);
		try {
			stmt.executeUpdate(sql);
			db.commit();
			logger.debug(Log.TEST, tablename + " has been dropped");
		}
		catch (SQLException e) {
			logger.warn(Log.TEST, tablename + " could not be dropped");
		}
	}
	
	static boolean tableExists(String tablename) throws SQLException {
		Database db = ResourceManager.getDatabase();
		return db.tableExists(tablename);
	}

	@Deprecated
	static void rollback() throws SQLException {
		ResourceManager.getDatabase().getConnection().rollback();		
	}
	
	static void commit() throws SQLException {
		Database db = ResourceManager.getDatabase();
		db.commit();
	}
	

	static int sqlCount(String tablename, String where) throws SQLException {
		String query = "select count(*) from " + tablename;
		if (where != null) query += " where " + where;
		return sqlCount(query);
	}
	
	static int sqlCount(String query) throws SQLException {		
		Database db = ResourceManager.getDatabase();
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

	
	static ResultSet getRow(String query) throws SQLException {
		Database db = ResourceManager.getDatabase();
		db.commit();
		Statement stmt = db.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		return rs;		
	}

	
	/*
	static String tableName(String name) throws IOException {
		if (name.indexOf(".") > -1) return name;
		String schema = AllTests.getSchema();
		String prefix = schema.length() > 0 ? schema + "." : schema;
		return prefix + name;
	}
				
	static String replace(String sql) throws IOException {
		String schema = AllTests.getSchema();
		String prefix = schema.length() > 0 ? schema + "." : schema;
		return sql.replaceAll("\\$", prefix);
	}
	
		
	static int numRows(String tablename) throws SQLException, IOException {
		String sql = "select count(*) from " + tableName(tablename);
		return sqlCount(sql);
	}
	
	static void sqlDeleteJupiter() throws IOException, SQLException {
		String tablename = tableName("cmn_location");
		sqlUpdate("delete from " + tablename +" where name='Jupiter'");
		connection.commit();
	}
	
	static int sqlCountJupiter() throws IOException, SQLException {
		return sqlCountTable("cmn_location", "where name='Jupiter'");
	}
	
	static int sqlCountTable(String name, String qualifier) throws IOException, SQLException {
		String tablename = tableName(name);
		String sql = "select count(*) from " + tablename;
		if (qualifier != null) sql = sql + " " + qualifier;
		return sqlCount(sql);
	}
	
	static int sqlCountTable(String name) throws IOException, SQLException {
		return sqlCountTable(name, null);
	}
	
	static void printReport(String query) throws SQLException {
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		int colCount = rs.getMetaData().getColumnCount();
		while (rs.next()) {
			StringBuilder line = new StringBuilder();
			for (int col = 1; col <= colCount; ++col) {
				String value = rs.getString(col);
				line.append(value);
				line.append(" ");
			}
			logger.info(line.toString());
		}
		
	}
	*/
	
}
