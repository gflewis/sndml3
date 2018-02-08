package servicenow.datamart;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import servicenow.core.Record;
import servicenow.core.Table;

public class TableInsertWriter extends TableWriter {

	public TableInsertWriter(String name, Database db, Table table, String sqlTableName) throws IOException, SQLException {
		super(name, db, table, sqlTableName);
	}

	@Override
	void writeRecord(Record rec) throws SQLException {
		try {
			insertStmt.insert(rec);
			writerMetrics.incrementInserted();
		}
		catch (SQLIntegrityConstraintViolationException e) {
			writerMetrics.incrementSkipped();
		}
	}

}
