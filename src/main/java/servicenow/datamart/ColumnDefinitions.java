package servicenow.datamart;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;

import servicenow.api.*;

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
		String dbschema = db.getSchema();
		String saveJob = Log.getJobContext();
		Log.setJobContext(sqlTableName + ".schema");
		logger.debug(Log.SCHEMA, String.format("schema=%s table=%s", dbschema, sqlTableName));
		Generator generator = db.getGenerator();
		TableWSDL wsdl = table.getWSDL();
		ResultSet columns = db.getColumnDefinitions(sqlTableName);
		while (columns.next()) {
			String name = columns.getString(4);
			int type = columns.getInt(5);
			int size = columns.getInt(7);
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
		if (this.size() < 1)
			throw new RuntimeException(
				"SQL table not found: " + db.qualifiedName(sqlTableName));
		if (!this.get(0).getName().toLowerCase().equals("sys_id"))
			throw new RuntimeException(
				String.format(
					"expected 'sys_id', found '%s' in first column of table '%s'",
					this.get(0).getName(), sqlTableName));
		logger.debug(Log.SCHEMA, this.size() + " columns");
		columns.close();
		Log.setJobContext(saveJob);	
	}
}
