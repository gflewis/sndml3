package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.*;

public class DatabaseDeleteWriter extends DatabaseTableWriter {

	protected DatabaseDeleteStatement deleteStmt;
		
	public DatabaseDeleteWriter(Database db, Table table, String sqlTableName, ProgressLogger progressLogger)
			throws IOException, SQLException {
		super(db, table, sqlTableName, progressLogger);
		progressLogger.setOperation("Deleted");
	}

	@Override
	public DatabaseDeleteWriter open() throws SQLException, IOException {
		super.open();
		deleteStmt = new DatabaseDeleteStatement(this.db, this.sqlTableName);
		return this;
	}
	
	@Override
	void writeRecord(Record rec) throws SQLException {
		assert rec.getTable().getName().equals("sys_audit_delete");
		Key key = rec.getKey("documentkey");
		assert key != null;
		deleteRecord(key);
	}
	
	void deleteRecords(KeySet keys) throws SQLException {
		for (Key key : keys) {
			deleteRecord(key);
		}
		db.commit();
	}
	
	private void deleteRecord(Key key) throws SQLException {
		logger.info(Log.PROCESS, "Delete " + key);		
		if (deleteStmt.deleteRecord(key)) {
			writerMetrics.incrementDeleted();			
		}
		else {
			logger.warn(Log.PROCESS, "Delete: Not found: " + key);
			writerMetrics.incrementSkipped();
		}
	}

}
