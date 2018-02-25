package servicenow.datamart;

import java.io.IOException;
import java.sql.SQLException;

import servicenow.api.*;

public class TableDeleteWriter extends TableWriter {

	public TableDeleteWriter(String name, Database db, Table table, String sqlTableName) throws IOException, SQLException {
		super(name, db, table, sqlTableName);
	}

	@Override
	void writeRecord(Record rec) throws SQLException {
		assert rec.getTable().getName().equals("sys_audit_delete");
		Key key = rec.getKey("document_key");
		if (deleteStmt.deleteRecord(key))
			writerMetrics.incrementDeleted();
		else
			writerMetrics.incrementSkipped();
	}

}
