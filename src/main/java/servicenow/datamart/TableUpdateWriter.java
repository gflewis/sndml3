package servicenow.datamart;

import java.io.IOException;
import java.sql.SQLException;

import servicenow.core.*;

public class TableUpdateWriter extends TableWriter {

	public TableUpdateWriter(Database db, Table table, String sqlTableName) throws IOException, SQLException {
		super(db, table, sqlTableName);
	}

	@Override
	void writeRecord(Record rec) throws SQLException {
		if (updateStmt.update(rec)) {
			metrics.incrementUpdated();
		} else {
			insertStmt.insert(rec);
			metrics.incrementInserted();
		}
	}

}
