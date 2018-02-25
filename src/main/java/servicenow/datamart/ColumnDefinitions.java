package servicenow.datamart;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;

import servicenow.api.*;

public class ColumnDefinitions extends ArrayList<SqlFieldDefinition> {

	private static final long serialVersionUID = 1L;

	final private Logger logger = Log.logger(this.getClass());

	/**
	 * Read the schema for a SQL table from the database.
	 * 
	 */	
	public ColumnDefinitions(Database db, Table table, String sqlTableName) 
			throws SQLException, IOException {
		super();		
		String schema = db.getSchema();
		logger.debug(Log.SCHEMA, String.format("schema=%s table=%s", schema, sqlTableName));
		Generator generator = db.getGenerator();
		TableWSDL wsdl = table.getWSDL();

		Log.setSchemaContext(table);		
		ResultSet columns = db.getColumnDefinitions(sqlTableName);
		while (columns.next()) {
			String name = columns.getString(4);
			int type = columns.getInt(5);
			int size = columns.getInt(7);
			String glidename = generator.glideName(name);
			if (wsdl.canReadField(glidename)) {
				SqlFieldDefinition defn =
					new SqlFieldDefinition(name, type, size, glidename);
				this.add(defn);
				logger.debug(Log.SCHEMA, name + " type=" + type + " size=" + size);				
			}
			else {
				logger.warn(Log.SCHEMA, name + " type=" + type + " size=" + size + " (not mapped)");				
			}				
		}
		if (this.size() < 1)
			throw new TableInitException(
				"SQL table not found: " + db.qualifiedName(sqlTableName));
		if (!this.get(0).getName().toUpperCase().equals("SYS_ID"))
			throw new RuntimeException(
				"expected SYS_ID, found " + this.get(0).getName() + 
				" in first column of table \"" + sqlTableName + "\"");
		logger.debug(Log.SCHEMA, this.size() + " columns");
		columns.close();
	
	}
}
