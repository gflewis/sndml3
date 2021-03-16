package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.*;

public class DatabaseUpdateWriter extends DatabaseTableWriter {

	protected DatabaseInsertStatement insertStmt;
	protected DatabaseUpdateStatement updateStmt;
	
	public DatabaseUpdateWriter(Database db, Table table, String sqlTableName, String writerName) 
			throws IOException, SQLException {
		super(db, table, sqlTableName, writerName);
	}

	@Override
	public DatabaseUpdateWriter open() throws SQLException, IOException {
		super.open();
		insertStmt = new DatabaseInsertStatement(this.db, this.sqlTableName, columns);
		updateStmt = new DatabaseUpdateStatement(this.db, this.sqlTableName, columns);
		return this;
	}
		
	@Override
	void writeRecord(Record rec) throws SQLException {
		Key key = rec.getKey();
		logger.trace(Log.PROCESS, "Update " + key);
		if (updateStmt.update(rec)) {
			writerMetrics.incrementUpdated();
		} else {
			logger.trace(Log.PROCESS, "Insert " + key);
			insertStmt.insert(rec);
			writerMetrics.incrementInserted();
		}
	}

}
