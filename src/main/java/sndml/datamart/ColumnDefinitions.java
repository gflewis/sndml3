package sndml.datamart;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;

import sndml.servicenow.*;

/**
 * Contains SQL data type information for all columns in the table which are readable
 * per the WSDL. 
 */
public class ColumnDefinitions extends ArrayList<DatabaseFieldDefinition> {

	private static final long serialVersionUID = 1L;

	final private Logger logger = Log.logger(this.getClass());

	/**
	 * Generate SQL data type information for all columns in the table
	 * which are readable per the WSDL.
	 * 
	 * @param db SQL database from which metadata is collected.
	 * @param table ServiceNow table whose WSDL will be used to establish which columns are readable.
	 * @param sqlTableName Name of the SQL table for which metadata is collected.
	 * @throws SQLException
	 * @throws IOException
	 */
	public ColumnDefinitions(Database db, Table table, String sqlTableName) 
			throws SQLException, IOException {
		super();
		assert db != null;
		assert table != null;
		assert sqlTableName != null;
		String dbschema = db.getSchema();
		String saveJob = Log.getJobContext();
		Log.setJobContext(sqlTableName + ".schema");
		logger.debug(Log.SCHEMA, String.format("schema=%s table=%s", dbschema, sqlTableName));
		Generator generator = db.getGenerator();
		TableWSDL wsdl = table.getWSDL();
		ResultSet rsColumns = getColumnDefinitions(db, sqlTableName);
		while (rsColumns.next()) {
			String name = rsColumns.getString(4);
			int type = rsColumns.getInt(5);
			int size = rsColumns.getInt(7);
			String glidename = generator.glideName(name);
			if (wsdl.canReadField(glidename)) {
				DatabaseFieldDefinition defn =
					new DatabaseFieldDefinition(name, type, size, glidename);
				this.add(defn);
				logger.trace(Log.SCHEMA, name + " type=" + type + " size=" + size);				
			}
			else {
				logger.warn(Log.SCHEMA, name + " type=" + type + " size=" + size + " (not mapped)");				
			}				
		}
		rsColumns.close();
		if (this.size() < 1)
			throw new RuntimeException(
				"SQL table not found: " + db.qualifiedName(sqlTableName));
		if (!this.get(0).getName().toLowerCase().equals("sys_id"))
			// DatabaseUpdateStatement assumes sys_id is first columm
			throw new RuntimeException(
				String.format(
					"expected 'sys_id', found '%s' in first column of table '%s'",
					this.get(0).getName(), sqlTableName));
		logger.debug(Log.SCHEMA, this.size() + " columns");
		Log.setJobContext(saveJob);	
	}
	
	private ResultSet getColumnDefinitions(Database database, String tablename) 
			throws SQLException {
		assert tablename != null;
		assert tablename.length() > 0;
		DatabaseMetaData meta = database.getConnection().getMetaData();
		String catalog, schema;
		if (database.isMySQL()) {
			catalog = database.getSchema();
			schema = null;
		}
		else {
			catalog = null;
			schema = database.getSchema();
		}
		if (database.isOracle()) tablename = tablename.toUpperCase();
		ResultSet rsColumns = meta.getColumns(catalog, schema, tablename, null);
		return rsColumns;
	}
	
}
