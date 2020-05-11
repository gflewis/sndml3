package servicenow.datamart;

import servicenow.api.*;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class DBTest {
	
	@Parameters(name = "{index}:{0}")
	public static String[] profiles() {
		return TestingManager.allProfiles();
	}
	
	static Logger logger = TestingManager.getLogger(DBTest.class);
	
	public DBTest(String profile) throws Exception {
		TestingManager.loadProfile(profile, true);
//		session = ResourceManager.getSession();
//		database = ResourceManager.getDatabase();
	}
		
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		ResourceManager.getDatabase().close();
	}

	static void initialize() throws TestingException, SQLException {
		Database db = ResourceManager.getDatabase();
		assert db != null;
		assert db.getConnection() != null;
	}

//	static Connection getConnection() throws SQLException {
//		Database db = ResourceManager.getDatabase();
//		return db.getConnection();
//	}
	
	static int sqlUpdate(String sql) throws SQLException {
		Database db = ResourceManager.getDatabase();
		Connection dbc = db.getConnection();
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
	
	@Test
	public void testTableExistsTrue() throws Exception {
		TestingManager.bannerStart(this.getClass(), "testTableExistsTrue");
		// logger.info("testTableExistsTrue");
		Database db = ResourceManager.getDatabase();
		String tablename = "core_company";		
		Table table = ResourceManager.getSession().table(tablename);
		assertNotNull(db);
		db.createMissingTable(table, tablename);
		assertTrue(db.tableExists(tablename));
		assertFalse(db.tableExists(tablename.toUpperCase()));
	}
	
	@Test
	public void testTableExistsFalse() throws Exception {
		TestingManager.bannerStart(this.getClass(), "testTableExistsFalse");
		// logger.info("testTableExistsFalse");
		Database db = ResourceManager.getDatabase();
		String tablename = "some_nonexistent_table";
		assertNotNull(db);
		assertFalse(db.tableExists(tablename));
		assertFalse(db.tableExists(tablename.toUpperCase()));
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
