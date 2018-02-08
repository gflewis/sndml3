package servicenow.datamart;

import java.io.IOException;
import java.sql.SQLException;

import servicenow.core.*;

public class TableUpdateWriter extends TableWriter {

	public TableUpdateWriter(String name, Database db, Table table, String sqlTableName) throws IOException, SQLException {
		super(name, db, table, sqlTableName);
	}

	@Override
	void writeRecord(Record rec) throws SQLException {
		if (updateStmt.update(rec)) {
			writerMetrics.incrementUpdated();
		} else {
			insertStmt.insert(rec);
			writerMetrics.incrementInserted();
		}
	}

}
