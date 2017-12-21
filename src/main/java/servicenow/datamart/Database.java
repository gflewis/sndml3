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

import servicenow.core.*;

public class Database {

	private final Connection dbc;
	private final URI dbURI;
	private final String dialect;
	private final String schema;
	private final Generator generator;
	final private Logger logger = Log.logger(this.getClass());

	public final static Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	
	public Database(Properties props) throws SQLException, URISyntaxException {
		String dburl  = props.getProperty("datamart.url");
		this.dbURI = new URI(dburl);
		String dbuser = props.getProperty("datamart.username");
		String dbpass = props.getProperty("datamart.password", "");
		schema = props.getProperty("datamart.schema");
		dialect = props.getProperty("datamart.dialect");
		
		assert dburl != null;
		assert dbuser != null;
		assert schema==null || schema.length() > 0;
		logger.info(Log.INIT, String.format("database=%s user=%s schema=%s", dburl, dbuser, schema));
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
	
	@Deprecated
	String getDialectName() {
		return getGenerator().getDialectName();
	}
	
	String getSchema() {
		return this.schema;
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
		assert tablename != null;
		assert tablename.length() > 0;
		DatabaseMetaData meta = getConnection().getMetaData();
		String schema = getSchema();
		ResultSet rs = meta.getTables(null, schema, tablename, null);
		boolean result = (rs.next() ? true : false);
		rs.close();
		logger.debug(Log.INIT, String.format("tableExists schema=%s table=%s result=%b", schema, tablename, result));
		return result;
	}

	/**
	 * Create a table in the target database if it does not already exist.
	 * If the table already exists then do nothing.
	 * @throws InterruptedException 
	 */
	void createMissingTable(Table table, String sqlTableName) 
			throws SQLException, IOException, InterruptedException  {
		assert table != null;
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
