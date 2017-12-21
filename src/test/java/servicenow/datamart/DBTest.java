package servicenow.datamart;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import servicenow.core.TestingException;
import servicenow.core.TestingManager;
import servicenow.datamart.Database;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBTest {

	static private Database db = null;
	static private Logger logger = AllTests.getLogger(DBTest.class);
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		initialize();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		disconnect();
	}

	static void initialize() throws TestingException, SQLException {
		logger.info("initialize");
		Properties props = TestingManager.getDefaultProperties();
		try {
			db = new Database(props);
		} catch (URISyntaxException e) {
			throw new TestingException(e);
		}
		assert db != null;
		assert db.getConnection() != null;
	}

	static Connection getConnection() throws SQLException {
		initialize();
		return db.getConnection();
	}
	
	static int sqlUpdate(String sql) throws SQLException {
		initialize();
		Connection dbc = getConnection();
		assertNotNull(dbc);
		logger.debug("sqlUpdate \"" + sql + "\"");
		Statement stmt = dbc.createStatement();
		int count = 0;
		try {
			count = stmt.executeUpdate(sql);
			dbc.commit();
		} catch (SQLException e) {
			logger.error(sql, e);
			throw e;
		}
		logger.info(sql + " (" + count + ")");
		return count;
	}

	static boolean tableExists(String tablename) throws SQLException {
		return db.tableExists(tablename);
	}
	
	static void rollback() throws SQLException {
		if (db == null) return;
		db.getConnection().rollback();		
	}
	
	static void commit() throws SQLException {
		if (db == null) return;
		getConnection().commit();
	}
	
	static void disconnect() throws SQLException {
		logger.info("disconnect");
		getConnection().close();
		db = null;
	}
	
	@Test
	public void testTableExistsTrue() throws Exception {
		logger.info("testTableExistsTrue");
		String tablename = "core_company";
		assertNotNull(db);
		assertTrue(db.tableExists(tablename));
		assertFalse(db.tableExists(tablename.toUpperCase()));
	}
	
	@Test
	public void testTableExistsFalse() throws Exception {
		logger.info("testTableExistsFalse");
		String tablename = "some_nonexistent_table";
		assertNotNull(db);
		assertFalse(db.tableExists(tablename));
		assertFalse(db.tableExists(tablename.toUpperCase()));
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
	

	static void dropTable(String tablename) throws Exception {
		tablename = tableName(tablename);
		logger.debug("dropTable " + tablename);
		try {
			sqlUpdate("drop table " + tablename);
			commit();
			logger.debug("table " + tablename + " has been dropped");
		}
		catch (SQLException e) {}	
	}
		
	static int numRows(String tablename) throws SQLException, IOException {
		String sql = "select count(*) from " + tableName(tablename);
		return sqlCount(sql);
	}
	
	static int sqlCount(String query) throws SQLException, IOException {
		initialize();
		connection.commit();
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		int count = rs.getInt(1);
		rs.close();
		connection.commit();
		logger.info(query + " (" + count + ")");
		return count;
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
