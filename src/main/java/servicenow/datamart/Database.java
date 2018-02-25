package servicenow.datamart;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Properties;
import java.util.TimeZone;

import org.slf4j.Logger;

import servicenow.api.*;

public class Database {

	private final Connection dbc;
	private final URI dbURI;
	private final String dbuser;
	private final String dialect;
	private final String schema;
	private final Generator generator;
	final private Logger logger = Log.logger(this.getClass());

	public final static Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	
	public Database(Properties props) throws SQLException, URISyntaxException {
		String dburl  = props.getProperty("datamart.url");
		this.dbURI = new URI(dburl);
		this.dbuser = props.getProperty("datamart.username");
		String dbpass = props.getProperty("datamart.password", "");
		schema = props.getProperty("datamart.schema");
		dialect = props.getProperty("datamart.dialect");
		
		assert dburl != null;
		assert dbuser != null;
		assert schema==null || schema.length() > 0;
		String logmsg = "database=" + dburl + " user=" + dbuser;
		if (schema != null) logmsg += " schema=" + schema;
		logger.info(Log.INIT, logmsg);
		this.dbc = DriverManager.getConnection(dburl, dbuser, dbpass);
		if (dialect != null && dialect.length() > 0)
			this.generator = new Generator(dialect, schema);
		else
			this.generator = new Generator(dbURI, schema);
		this.initialize();
		assert this.dbc != null;
	}

	/**
	 * Initialize the database connection.
	 * Set the timezoneName to GMT.
	 * Set the date format to YYYY-MM-DD
	 */
	private void initialize() throws SQLException {
		dbc.setAutoCommit(false);
		Statement stmt = dbc.createStatement();
		Iterator<String> iter = generator.getInitializations().listIterator();
		while (iter.hasNext()) {
			String sql = iter.next();
			logger.info(Log.INIT, sql);
			stmt.execute(sql);
		}
		stmt.close();
		dbc.commit();
		// TODO: batch inserts
//		if (batchInserts) {
//			if (!meta.supportsBatchUpdates()) {
//				logger.warn("batch inserts not supported");
//				batchInserts = false;
//			}
//		}
//		logger.debug("batch_inserts=" + batchInserts);
	}
	
	
	boolean isOracle() {
		String protocol = getProtocol(getURI());
		return "oracle".equalsIgnoreCase(protocol);
	}
	
	static String getProtocol(URI uri) {
		String urlPart[] = uri.toString().split(":");
		String protocol = urlPart[1];
		return protocol;		
	}
	
	URI getURI() {
		return this.dbURI;
	}
	
	Connection getConnection() {
		assert this.dbc != null;
		return this.dbc;
	}
	
	Generator getGenerator() {
		return this.generator;
	}
		
	String getSchema() {
		String result;
		if (this.schema == null && this.isOracle()) 
			result = this.dbuser;
		else
			result = this.schema;
		if (this.isOracle()) result = result.toUpperCase();
		return result;
	}
	
	String qualifiedName(String name) {
		assert name != null;
		assert name.length() > 0;
		if (this.schema == null) 
			return name;
		else
			return this.schema + "." + name;
	}
	
	void executeStatement(String sqlCommand) throws SQLException {
		if (dbc == null) throw new IllegalStateException();
		Statement stmt = dbc.createStatement();
		logger.info(Log.PROCESS, sqlCommand);
		stmt.execute(sqlCommand);
		stmt.close();
	}
	
	void commit() throws SQLException {
		dbc.commit();
	}
	
	void truncateTable(String sqlTableName) throws SQLException {
		String sql = generator.getTemplate("truncate", sqlTableName);
		logger.info(Log.INIT, sql);
		executeStatement(sql);
	}
	
	/**
	 * Determine if a table already exists in the target database.
	 * 
	 * @return true if table exists; otherwise false
	 * @throws SQLException
	 */
	boolean tableExists(String tablename) 
			throws SQLException {
		/*
		assert tablename != null;
		assert tablename.length() > 0;
		DatabaseMetaData meta = getConnection().getMetaData();
		String schema = getSchema();
		if (this.isOracle()) tablename = tablename.toUpperCase();
		ResultSet rs = meta.getTables(null, schema, tablename, null);
		*/
		ResultSet rs = getTableDefinition(tablename);
		boolean result = (rs.next() ? true : false);
		rs.close();
		logger.debug(Log.INIT, String.format("tableExists schema=%s table=%s result=%b", schema, tablename, result));
		return result;
	}

	ResultSet getTableDefinition(String tablename) throws SQLException {
		assert tablename != null;
		assert tablename.length() > 0;
		DatabaseMetaData meta = getConnection().getMetaData();
		String schema = getSchema();
		if (this.isOracle()) tablename = tablename.toUpperCase();
		ResultSet rs = meta.getTables(null, schema, tablename, null);
		return rs;		
	}

	ResultSet getColumnDefinitions(String tablename) throws SQLException {
		assert tablename != null;
		assert tablename.length() > 0;
		DatabaseMetaData meta = getConnection().getMetaData();
		String schema = getSchema();
		if (this.isOracle()) tablename = tablename.toUpperCase();
		ResultSet rs = meta.getColumns(null, schema, tablename, null);
		return rs;		
	}
	
	
	/**
	 * Create a table in the target database if it does not already exist.
	 * If the table already exists then do nothing.
	 * @throws InterruptedException 
	 */
	void createMissingTable(Table table, String sqlTableName) 
			throws SQLException, IOException, InterruptedException  {
		assert table != null;
		Log.setTableContext(table);
		if (sqlTableName == null) sqlTableName = table.getName();
		if (tableExists(sqlTableName)) return;
		Statement stmt = dbc.createStatement();
		String createSql = generator.getCreateTable(table, sqlTableName);
		logger.info(Log.INIT, createSql);
		try {
			stmt.execute(createSql);
		} catch (SQLException e) {
			logger.error(Log.INIT, createSql, e);
			throw e;
		}
		String grantSql = generator.getTemplate("grant", sqlTableName);
		if (grantSql.length() > 0) {
			logger.info(Log.INIT, grantSql);
			try { 
				stmt.execute(grantSql);		
			} catch (SQLException e) {
				logger.error(Log.INIT, grantSql, e);
				throw e;
			}
		}
		stmt.close();
		commit();
	}
	
}
