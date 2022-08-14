package sndml.datamart;

import java.io.File;
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
import java.util.TimeZone;

import org.slf4j.Logger;

import sndml.servicenow.*;

/**
 * <p>Encapsulates a connection to a JDCB database (<tt>javasql.Connection</tt>).
 * </p>
 * <p>When this object is instantiated, it will execute any SQL statements
 * from the <tt>&lt;initialize&gt;</tt> section of <tt>sqltemplates.xml</tt>.
 * These statements should be used to ensure that the session time zone is GMT
 * and the date format is <tt>YYYY-MM-DD HH24:MI:SS</tt>.
 * </p>
 * 
 */
public class Database {

	private final Logger logger = Log.logger(this.getClass());
	private final ConnectionProfile profile;
	private final String dburl;
	private final URI dbURI;
	private final String protocol;
	private final Calendar calendar;
	private final String dbuser;
	private final String dbpass;
	private final boolean warnOnTruncate;
	private final String schema;
	private final File templates;
	
	private Connection dbc = null;
	private Generator generator;

	public final static Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	
	public Database(ConnectionProfile profile) throws SQLException, URISyntaxException {
		this.profile = profile;
		this.dburl = databaseProperty("url", null);
		this.dbURI = new URI(dburl);
		this.protocol = getProtocol(this.dbURI);
		this.dbuser = databaseProperty("username", null);
		this.dbpass = databaseProperty("password", "");
		schema = profile.getProperty("schema", null);
		
		assert dbc == null;
		assert dburl != null;
		assert schema==null || schema.length() > 0;

		// If timezone is not specified then use "GMT"
		// If timezone is "default" then use time zone of virtual machine
		String timezone = databaseProperty("timezone", "GMT");
		this.calendar = 
			timezone.equalsIgnoreCase("default") ? null :
			Calendar.getInstance(TimeZone.getTimeZone(timezone));
		
		String logmsg = "database=" + dburl;
		logmsg += " " + timezone;
		logmsg += " user=" + dbuser;
		
		if (schema != null) logmsg += " schema=" + getSchema();
				
		logger.info(Log.INIT, logmsg);
		String templateName = profile.getProperty("datamart.templates", "");
		this.templates = (templateName.length() > 0) ? new File(templateName) : null;
		this.warnOnTruncate = profile.getPropertyBoolean("loader.warn_on_truncate", true);
				
		this.open();
		assert dbc != null;
	}

	private String databaseProperty(String name, String defaultValue) {
		// Allow property to begin with old prefix "datamart." or new prefix "database."
		String value = 
			profile.getProperty("database." + name,
				profile.getProperty("datamart." + name, defaultValue));
		return value;
	}
	
	/**
	 * Open the database connection.
	 * Set the timezoneName to GMT.
	 * Set the date format to YYYY-MM-DD
	 */
	void open() throws SQLException {		
		dbc = DriverManager.getConnection(dburl, dbuser, dbpass);
		generator = new Generator(this, this.profile, this.templates);
		dbc.setAutoCommit(generator.getAutoCommit());
		Statement stmt = dbc.createStatement();
		Iterator<String> iter = generator.getInitializations().listIterator();
		while (iter.hasNext()) {
			String sql = iter.next();
			logger.info(Log.INIT, sql);
			stmt.execute(sql);
		}
		stmt.close();
		commit();		
	}
		
	public void close() throws SQLException {
		logger.info(Log.FINISH, "Database connection closed");
		this.dbc.close();
		this.dbc = null;
		assert this.isClosed();
	}

	boolean isClosed() {
		return (this.dbc == null);
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
	
	URI getURI() {
		return this.dbURI;
	}
	
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
		String catalog = null, schema = null;
		if (isMySQL()) {
			catalog = getSchema();
		}
		else {
			schema = getSchema();
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
