package sndml.datamart;

import java.sql.SQLException;
import java.util.HashMap;

import sndml.servicenow.*;

public class DatabaseDeleteStatement extends DatabaseStatement {

	public DatabaseDeleteStatement(Database db, String sqlTableName) throws SQLException {
		super(db, "delete", sqlTableName, null);
	}
	
	@Override
	String buildStatement() throws SQLException {
		HashMap<String,String> map = new HashMap<String,String>();
		map.put("keyvalue", "?");
		return generator.getTemplate(templateName, sqlTableName, map);	
	}

	public boolean deleteRecord(RecordKey key) throws SQLException {
		this.bindField(1, key.toString());
		int count = stmt.executeUpdate();
		return (count > 0);
	}
}
