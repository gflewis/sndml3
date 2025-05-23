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
import java.util.TimeZone;

import org.slf4j.Logger;

import sndml.servicenow.*;
import sndml.util.FieldNames;
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

	private final String protocol;
	private final Calendar calendar;
	private String timezone;
	private String dbuser;
	private boolean warnOnTruncate;
	private String schema;
	
	private final Connection connection;
	private final Generator generator;

	public final static Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	private final static Logger logger = Log.getLogger(DatabaseWrapper.class);

	public DatabaseWrapper(Connection connection, ConnectionProfile profile, Generator generator) throws SQLException {
		this.connection = connection;
		this.generator = generator;
		assert this.generator != null;
		assert this.connection != null;
		this.protocol = protocolFromProfile(profile);
		assert protocol != null : "protocol not found";
		this.schema = profile.getProperty("database.schema");
		// schema is allowed to be null
		assert schema==null || schema.length() > 0 : "invalid schema";
		this.dbuser = null;
		// The default time zone is "GMT"
		// If timezone is "local" then use time zone of virtual machine
		// "default" is deprecated. Please use "local" instead
		timezone = profile.getProperty("database.timezone");
		if (timezone.equalsIgnoreCase("local") || timezone.equalsIgnoreCase("default"))
			this.calendar = null;
		else
			this.calendar = Calendar.getInstance(TimeZone.getTimeZone(timezone));
		this.warnOnTruncate = profile.getBoolean("database.warn_on_truncate"); 
		if (timezone == "GMT")
			logger.debug(Log.INIT, String.format("timezone=%s warn_on_truncate=%b", timezone, warnOnTruncate));
		else 
			logger.info(Log.INIT, String.format("timezone=%s warn_on_truncate=%b", timezone, warnOnTruncate));			
		initialize();
	}
	
	public DatabaseWrapper(ConnectionProfile profile, Generator generator) throws SQLException {
		this(newConnectionFromProfile(profile), profile, generator);
		this.dbuser = profile.getProperty("database.username");
		schema = profile.getProperty("database.schema");		
		assert schema==null || schema.length() > 0;										
	}

	// Used for JUnit tests
	@Deprecated
	DatabaseWrapper(ConnectionProfile profile) 
			throws ResourceException, SQLException {
		this(profile, new Generator(profile));
	}
	
	public static String protocolFromProfile(ConnectionProfile profile) {
		if (profile.hasProperty("database.dialect"))
			return profile.getProperty("database.dialect");
		else {
			String dburl = profile.getProperty("database.url");
			assert dburl != null : "Property database.url not found";
			URI dbURI;
			try {
				dbURI = new URI(dburl);
			} catch (URISyntaxException e) {
				throw new ResourceException(e);
			}
			return protocolFromURI(dbURI);			
		}
	}
	
	private static Connection newConnectionFromProfile(ConnectionProfile profile) throws SQLException {
		PropertySet properties = profile.database;
		String dburl = properties.getProperty("url", null);
		String dbuser = properties.getProperty("username", null);
		String dbpass = properties.getProperty("password", "");
		assert dburl != null : "Property database.url not found";
		String logmsg = "database=" + dburl;
		logmsg += " user=" + dbuser;
		logger.info(Log.INIT, logmsg);		
		Connection dbc = DriverManager.getConnection(dburl, dbuser, dbpass);
		return dbc;		
	}
	
	public void initialize() throws SQLException {
		generator.initialize(connection);
	}

	@Override
	public void finalize() {
		try {
			this.connection.close();
		} catch (SQLException e) {
			// Ignore
		}		
	}
	
	public void close() throws SQLException {
		if (!this.connection.isClosed()) {
			logger.info(Log.FINISH, "Database connection closed");
			this.connection.close();			
		}
	}

	boolean isClosed() throws SQLException {
		return this.connection.isClosed();
	}
	
	Generator getGenerator() {
		return this.generator;
	}
	
	boolean isOracle() {
		return "oracle".equalsIgnoreCase(protocol);
	}
	
	boolean isMicrosoft() {
		return "sqlserver".equalsIgnoreCase(protocol);
	}
	
	boolean isPostgreSQL() {
		return "postgresql".equalsIgnoreCase(protocol);		
	}
	
	boolean isMySQL() {
		return "mysql".equalsIgnoreCase(protocol);
	}
	
	boolean isSQLite() {
		return "sqlite".equalsIgnoreCase(protocol);
	}
		
	boolean isAutoCommitEnabled() {
		return generator.getAutoCommit();
	}
	
	boolean getWarnOnTruncate() {
		return this.warnOnTruncate;
	}
		
	String getProtocol() {
		return this.protocol;
	}
	
	static String protocolFromURI(URI dbURI) {
		String urlPart[] = dbURI.toString().split(":");
		return urlPart[1];		
	}
	
	Calendar getCalendar() {
		return this.calendar;
	}
	
	Connection getConnection() {
		assert this.connection != null;
		return this.connection;
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
		if (connection == null) throw new IllegalStateException();
		Statement stmt = connection.createStatement();
		logger.info(Log.PROCESS, sqlCommand);
		stmt.execute(sqlCommand);
		stmt.close();
	}
	
	void commit() throws SQLException {
		if (!generator.getAutoCommit()) connection.commit();
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
			Statement stmt = connection.createStatement();
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
		Statement stmt = connection.createStatement();
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
