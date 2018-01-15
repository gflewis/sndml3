package servicenow.datamart;

import servicenow.core.*;

import java.io.IOException;
import java.sql.SQLException;

import servicenow.core.Record;
import servicenow.core.Table;

public class TableDeleteWriter extends TableWriter {

	public TableDeleteWriter(Database db, Table table, String sqlTableName) throws IOException, SQLException {
		super(db, table, sqlTableName);
	}

	@Override
	void writeRecord(Record rec) throws SQLException {
		assert rec.getTable().getName().equals("sys_audit_delete");
		Key key = rec.getKey("document_key");
		if (deleteStmt.deleteRecord(key))
			metrics.incrementDeleted();
		else
			metrics.incrementSkipped();
	}

}
