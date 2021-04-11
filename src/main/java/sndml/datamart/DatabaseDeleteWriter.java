package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.RecordKey;
import sndml.servicenow.KeySet;
import sndml.servicenow.Log;
import sndml.servicenow.Metrics;
import sndml.servicenow.ProgressLogger;
import sndml.servicenow.TableRecord;
import sndml.servicenow.Table;

public class DatabaseDeleteWriter extends DatabaseTableWriter {

	protected DatabaseDeleteStatement deleteStmt;
		
	public DatabaseDeleteWriter(Database db, Table table, String sqlTableName, String writerName)
			throws IOException, SQLException {
		super(db, table, sqlTableName, writerName);
	}

	@Override
	public DatabaseDeleteWriter open(Metrics writerMetrics) throws SQLException, IOException {
		super.open(writerMetrics);
		deleteStmt = new DatabaseDeleteStatement(this.db, this.sqlTableName);
//		progressLogger.setOperation("Deleted");
		return this;
	}
	
	@Override
	void writeRecord(TableRecord rec, Metrics writerMetrics) throws SQLException {
		assert rec.getTable().getName().equals("sys_audit_delete");
		RecordKey key = rec.getKey("documentkey");
		assert key != null;
		deleteRecord(key, writerMetrics);
	}
	
	void  deleteRecords(KeySet keys, Metrics writerMetrics, ProgressLogger progressLogger) 
			throws SQLException {
		for (RecordKey key : keys) {
			deleteRecord(key, writerMetrics);
		}
		db.commit();		
	}
	
	private void deleteRecord(RecordKey key, Metrics writerMetrics) throws SQLException {
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
