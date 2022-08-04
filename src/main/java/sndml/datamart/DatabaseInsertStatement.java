package sndml.datamart;

import java.sql.SQLException;
import java.util.HashMap;

import sndml.servicenow.TableRecord;

public class DatabaseInsertStatement extends DatabaseStatement {

	public DatabaseInsertStatement(Database db, String sqlTableName, ColumnDefinitions columns)
			throws SQLException {
		super(db, "insert", sqlTableName, columns);
	}

	String buildStatement() throws SQLException {
		final String fieldSeparator = ",\n";
		StringBuilder fieldnames = new StringBuilder();
		StringBuilder fieldvalues = new StringBuilder();
		for (int i = 0; i < columns.size(); ++i) {
			if (i > 0) {
				fieldnames.append(fieldSeparator);
				fieldvalues.append(",");
			}
			fieldnames.append(generator.sqlQuote(columns.get(i).getName()));
			fieldvalues.append("?");
		}
		HashMap<String,String> map = new HashMap<String,String>();
		map.put("fieldnames", fieldnames.toString());
		map.put("fieldvalues", fieldvalues.toString());
		return generator.getTemplate(templateName, sqlTableName, map);
	}
	
	public void insert(TableRecord rec) throws SQLException {
		setRecord(rec);
		int n = columns.size();
		for (int i = 0; i < n; ++i) {
			bindField(i + 1, i);
		}
		int count = stmt.executeUpdate();
		if (count > 1) throw new AssertionError("insert count=" + count);		
	}
		

}
