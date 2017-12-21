package servicenow.datamart;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import servicenow.core.Record;
import servicenow.core.Table;

public class TableInsertWriter extends TableWriter {

	public TableInsertWriter(Database db, Table table, String sqlTableName) throws IOException, SQLException {
		super(db, table, sqlTableName);
	}

	@Override
	void writeRecord(Record rec) throws SQLException {
		try {
			insertStmt.insert(rec);
			metrics.incrementInserted();
		}
		catch (SQLIntegrityConstraintViolationException e) {
			metrics.incrementSkipped();
		}
	}

}
