package sndml.loader;

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
import java.util.Properties;
import java.util.TimeZone;

import org.slf4j.Logger;

import sndml.servicenow.*;
import sndml.util.Log;
import sndml.util.PropertySet;
import sndml.util.ResourceException;

/**
 * <p>Encapsulates a connection to a JDCB database (<code>javasql.Connection</code>).
 * </p>
 * <p>When this object is instantiated, it will execute any SQL statements
 * from the <code>&lt;initialize&gt;</code> section of <code>sqltemplates.xml</code>.
 * These statements should be used to ensure that the session time zone is GMT
 * and the date format is <code>YYYY-MM-DD HH24:MI:SS</code>.
 * </p>
 * 
 */
public class DatabaseWrapper {

	private final Logger logger = Log.logger(this.getClass());
//	private final ConnectionProfile profile;
//	private final String dburl;
//	private final URI dbURI;
	private final String protocol;
	private final Calendar calendar;
	private final String dbuser;
//	private final String dbpass;
	private final boolean warnOnTruncate;
	private final String schema;
	
	private final Connection dbc;
	private final Generator generator;

	public final static Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

	public DatabaseWrapper(Connection connection, Generator generator, Properties properties) {
		this.protocol = properties.getProperty("dialect");
		this.schema = properties.getProperty("schema");
		this.dbuser = null;
		// If timezone is not specified then use "GMT"
		// If timezone is "default" then use time zone of virtual machine
		String timezone = properties.getProperty("timezone", "GMT");
		this.calendar = 
			timezone.equalsIgnoreCase("default") ? null :
			Calendar.getInstance(TimeZone.getTimeZone(timezone));
		this.warnOnTruncate = Boolean.parseBoolean(properties.getProperty("warn_on_truncate", "true"));
		this.dbc = connection;
		this.generator = generator;				
	}
	
	public DatabaseWrapper(ConnectionProfile profile) throws SQLException {
//		this.profile = profile;
		PropertySet properties = profile.database;
		String dburl = properties.getProperty("url", null);
		assert dburl != null : "Property database.url not found";
		URI dbURI;
		try {
			dbURI = new URI(dburl);
		} catch (URISyntaxException e) {
			throw new ResourceException(e);
		}
		this.protocol = getProtocol(dbURI);
		this.dbuser = properties.getProperty("username", null);
		String dbpass = properties.getProperty("password", "");
		schema = profile.database.getProperty("schema", null);
		
		assert schema==null || schema.length() > 0;

		// If timezone is not specified then use "GMT"
		// If timezone is "default" then use time zone of virtual machine
		String timezone = properties.getProperty("timezone", "GMT");
		this.calendar = 
			timezone.equalsIgnoreCase("default") ? null :
			Calendar.getInstance(TimeZone.getTimeZone(timezone));
		
		String logmsg = "database=" + dburl;
		logmsg += " " + timezone;
		logmsg += " user=" + dbuser;
		
		if (schema != null) logmsg += " schema=" + getSchema();
				
		logger.info(Log.INIT, logmsg);
		this.warnOnTruncate = profile.loader.getBoolean("warn_on_truncate", true);
				
		this.dbc = this.open(dburl, dbuser, dbpass);		
		assert dbc != null;
		
		this.generator = new Generator(profile);
	}
		
//	private String databaseProperty(String name, String defaultValue) {
//		// Allow property to begin with old prefix "datamart." or new prefix "database."
//		String value = profile.database.getProperty(name, defaultValue);
//		return value;
//	}
//	
	/**
	 * Open the database connection.
	 * Set the timezoneName to GMT.
	 * Set the date format to YYYY-MM-DD
	 */
	private Connection open(String dburl, String dbuser, String dbpass) throws SQLException {		
		Connection dbc = DriverManager.getConnection(dburl, dbuser, dbpass);
		generator.initialize(dbc);
		return dbc;
	}
	
	public void initialize() throws SQLException {
		generator.initialize(dbc);
	}

	@Override
	public void finalize() {
		try {
			this.dbc.close();
		} catch (SQLException e) {
			// Ignore
		}		
	}
	
	public void close() throws SQLException {
		logger.info(Log.FINISH, "Database connection closed");
		this.dbc.close();
	}

	boolean isClosed() throws SQLException {
		return this.dbc.isClosed();
	}
	
	Generator getGenerator() {
		return this.generator;
	}
	
	boolean isOracle() {
		return "oracle".equalsIgnoreCase(protocol);
	}
	
	boolean isMSSQL() {
		return "sqlserver".equalsIgnoreCase(protocol);
	}
	
	boolean isPostgreSQL() {
		return "postgresql".equalsIgnoreCase(protocol);		
	}
	
	boolean isMySQL() {
		return "mysql".equalsIgnoreCase(protocol);
	}
		
	boolean isAutoCommitEnabled() {
		return generator.getAutoCommit();
	}
	
	boolean getWarnOnTruncate() {
		return this.warnOnTruncate;
	}
	
//	@Deprecated
//	URI getURI() {
//		return this.dbURI;
//	}
	
	String getProtocol() {
		return this.protocol;
	}
	
	static String getProtocol(URI dbURI) {
		String urlPart[] = dbURI.toString().split(":");
		return urlPart[1];		
	}
	
	Calendar getCalendar() {
		return this.calendar;
	}
	
	Connection getConnection() {
		assert this.dbc != null;
		return this.dbc;
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
		if (!generator.getAutoCommit()) dbc.commit();
	}
	
	void truncateTable(String sqlTableName) throws SQLException {
		String sql = generator.getTemplate("truncate", sqlTableName);
		logger.info(Log.INIT, sql);
		executeStatement(sql);
		commit();
	}
	
	/**
	 * Determine if a table already exists in the target database.
	 * 
	 * @return true if table exists; otherwise false
	 * @throws SQLException
	 */
	boolean tableExists(String tablename) 
			throws SQLException {
		assert tablename != null;
		assert tablename.length() > 0;
		DatabaseMetaData meta = getConnection().getMetaData();
		String catalog = null;
		String schema = getSchema();
		// Why are these reversed for MySQL?
		if (isMySQL()) {
			catalog = schema;
			schema = null;
		}
		if (isOracle()) tablename = tablename.toUpperCase();
		ResultSet rsTables = meta.getTables(catalog, schema, tablename, null);
		int count = 0;
		while (rsTables.next()) {
			count += 1;
			logger.trace(Log.INIT, String.format(
				"getTableDefinition (%d) catalog=%s schema=%s tablename=%s type=%s",
				count, rsTables.getString(1), rsTables.getString(2), 
				rsTables.getString(3), rsTables.getString(4)));
		}
		rsTables.close();
		if (count > 1) 
			throw new ResourceException(String.format(
				"DatabaseMetaData returned %d rows for catalog=%s schema=%s tablename=%s", 
				count, catalog, schema, tablename));
		boolean result = count > 0 ? true : false;
		logger.debug(Log.INIT, String.format(
				"tableExists protocol=%s schema=%s table=%s result=%b", 
				protocol, getSchema(), tablename, result));
		return result;
	}

	/**
	 * <p>Drop a database table if exists.</p>
	 * <p>This method is used for JUnit tests. It will always generate a warning in the log.</p>
	 * @param sqlTableName Name of the table to be dropped.
	 * @param addSchema If true then schema prefix will be added to the table name.
	 */
	void dropTable(String sqlTableName, boolean addSchema) 
			throws SQLException {
		if (tableExists(sqlTableName)) {
			String fullName = addSchema ? this.qualifiedName(sqlTableName) : sqlTableName;
			logger.warn(Log.INIT, String.format("dropTable: %s", fullName));
			String sql = "DROP TABLE " + fullName;
			Statement stmt = dbc.createStatement();
			try {
				stmt.execute(sql);			
			}
			catch (SQLException e) {
				logger.error(Log.INIT, sql, e);
				throw e;
			}
		}
		else {
			logger.warn(Log.INIT, String.format("dropTable: not found: %s", sqlTableName));
		}
	}
	
	void createTable(Table table, String sqlTableName, FieldNames columns)
			throws SQLException, IOException, InterruptedException {
		logger.debug(Log.INIT, String.format(
			"createTable source=%s target=%s", table.getName(), sqlTableName));
		assert table != null;
		assert sqlTableName != null;
		Log.setTableContext(table);
		Statement stmt = dbc.createStatement();
		String createSql = generator.getCreateTable(table, sqlTableName, columns);
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
	
	/**
	 * Create a table in the target database if it does not already exist.
	 * If the table already exists then do nothing.
	 */

	void createMissingTable(Table table) 
			throws SQLException, IOException, InterruptedException {
		createMissingTable(table, table.getName());
	}
	
	void createMissingTable(Table table, String sqlTableName) 
			throws SQLException, IOException, InterruptedException {
		createMissingTable(table, sqlTableName, null);
	}

	void createMissingTable(Table table, String sqlTableName, FieldNames columns) 
			throws SQLException, IOException, InterruptedException  {
		assert table != null;
		if (sqlTableName == null) sqlTableName = table.getName();
		// logger.debug(Log.INIT, "createMissingTable " + sqlTableName + " checking if table exists");
		boolean exists = tableExists(sqlTableName);
		logger.debug(Log.INIT, "createMissingTable " + sqlTableName + " exists=" + exists);
		if (!exists) {
			createTable(table, sqlTableName, columns);
		}
	}
		
}
