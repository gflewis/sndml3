package servicenow.datamart;

import servicenow.core.*;

import java.sql.SQLException;
import java.util.HashMap;

public class DeleteStatement extends SqlStatement {

	public DeleteStatement(Database db, String sqlTableName) throws SQLException {
		super(db, "delete", sqlTableName, null);
	}
	
	@Override
	String buildStatement() throws SQLException {
		HashMap<String,String> map = new HashMap<String,String>();
		map.put("keyvalue", "?");
		return generator.getTemplate(templateName, sqlTableName, map);	
	}

	public boolean deleteRecord(Key key) throws SQLException {
		this.bindField(1,  key.toString());
		int count = stmt.executeUpdate();
		return (count > 0);
	}
}
