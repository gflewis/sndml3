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
import java.util.Properties;
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
	private final URI dbURI;
	private final String protocol;
	private final String dbuser;
	private final boolean autocommit;
	private final boolean warnOnTruncate;
	private final String schema;
	private final Properties properties = new Properties();
	private final File templates;
	private final Generator generator;
	private Connection dbc = null;

	public final static Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	
	public Database(Properties props) throws SQLException, URISyntaxException {
		this.properties.putAll(props);
		String dburl = props.getProperty("datamart.url");
		this.dbURI = new URI(dburl);
		this.protocol = getProtocol(this.dbURI);
		this.dbuser = props.getProperty("datamart.username");
		String dbpass = props.getProperty("datamart.password", "");
		schema = props.getProperty("datamart.schema");
		
		assert dbc == null;
		assert dburl != null;
		assert dbuser != null;
		assert schema==null || schema.length() > 0;
		String logmsg = "database=" + dburl + " user=" + dbuser;
		if (schema != null) logmsg += " schema=" + getSchema();
		logger.info(Log.INIT, logmsg);
		this.dbc = DriverManager.getConnection(dburl, dbuser, dbpass);
		this.warnOnTruncate = new Boolean(props.getProperty("loader.warn_on_truncate", "true"));
		this.templates = (props.getProperty("datamart.templates", "").length() > 0) ?
			new File(props.getProperty("datamart.templates")) : null;
		this.generator = new Generator(this, this.properties, this.templates);
		this.autocommit = this.generator.getAutoCommit();
		this.initialize();
		assert dbc != null;
	}

	/**
	 * Initialize the database connection.
	 * Set the timezoneName to GMT.
	 * Set the date format to YYYY-MM-DD
	 */
	private void initialize() throws SQLException {
		
		dbc.setAutoCommit(this.autocommit);
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
		
	void close() throws SQLException {
		logger.info(Log.FINISH, "Database connection closed");
		this.dbc.close();
		this.dbc = null;
		assert this.isClosed();
	}
	
	Properties getProperties() {
		return this.properties;
	}

	Generator getGenerator() {
		return this.generator;
	}
	
	boolean isOracle() {
		return "oracle".equalsIgnoreCase(protocol);
	}
	
	boolean isPostgresql() {
		return "postgresql".equalsIgnoreCase(protocol);		
	}
	
	boolean isMySQL() {
		return "mysql".equalsIgnoreCase(protocol);
	}
	
	boolean isAutoCommitEnabled() {
		return this.autocommit;
	}
	
	boolean getWarnOnTruncate() {
		return this.warnOnTruncate;
	}
	
	boolean isClosed() {
		return (this.dbc == null);
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
		if (!autocommit) dbc.commit();
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
	
	void createTable(Table table, String sqlTableName)
			throws SQLException, IOException, InterruptedException {
		assert table != null;
		assert sqlTableName != null;
		Log.setTableContext(table);
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
	
	/**
	 * Create a table in the target database if it does not already exist.
	 * If the table already exists then do nothing.
	 */
	void createMissingTable(Table table, String sqlTableName) 
			throws SQLException, IOException, InterruptedException  {
		assert table != null;
		if (sqlTableName == null) sqlTableName = table.getName();
		if (tableExists(sqlTableName)) {
			return;
		}
		else {
			createTable(table, sqlTableName);			
		}
	}
	
	void createMissingTable(Table table) 
			throws SQLException, IOException, InterruptedException {
		createMissingTable(table, table.getName());
	}
	
}
