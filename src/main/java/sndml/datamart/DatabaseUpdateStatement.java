package sndml.datamart;

import java.sql.SQLException;
import java.util.HashMap;

import sndml.servicenow.BaseRecord;

public class DatabaseUpdateStatement extends DatabaseStatement {

	public DatabaseUpdateStatement(Database db, String sqlTableName, ColumnDefinitions columns)
			throws SQLException {
		super(db, "update", sqlTableName, columns);
	}

	String buildStatement() throws SQLException {
		final String fieldSeparator = ",\n";
		StringBuilder fieldmap = new StringBuilder();
		for (int i = 1; i < columns.size(); ++i) {
			if (i > 1) 	fieldmap.append(fieldSeparator);
			fieldmap.append(generator.sqlQuote(columns.get(i).getName()));
			fieldmap.append("=?");						
		}
		HashMap<String,String> map = new HashMap<String,String>();
		map.put("fieldmap", fieldmap.toString());
		map.put("keyvalue", "?");
		return generator.getTemplate(templateName, sqlTableName, map);
	}
	
	public boolean update(BaseRecord rec) throws SQLException {
		this.lastRec = rec;
		int n = columns.size();
		// Skip column 0 which is the sys_id
		for (int i = 1; i < n; ++i) {
			bindField(i, rec, i);
		}
		// Bind sys_id to the last position
		bindField(n, rec, columns.get(0), rec.getKey().toString());
		int count = stmt.executeUpdate();
		if (count > 1) throw new AssertionError("update count=" + count);
		return (count > 0);		
	}
			
}
