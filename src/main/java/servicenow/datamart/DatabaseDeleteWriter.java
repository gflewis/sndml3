package servicenow.datamart;

import servicenow.api.*;

import java.io.IOException;
import java.sql.SQLException;

public class DatabaseDeleteWriter extends DatabaseTableWriter {

	protected DatabaseDeleteStatement deleteStmt;
		
	public DatabaseDeleteWriter(Database db, Table table, String sqlTableName) throws IOException, SQLException {
		super(db, table, sqlTableName);
	}

	@Override
	public void open() throws SQLException, IOException {
		super.open();
		deleteStmt = new DatabaseDeleteStatement(this.db, this.sqlTableName);
		
	}
	
	@Override
	void writeRecord(Record rec) throws SQLException {
		assert rec.getTable().getName().equals("sys_audit_delete");
		Key key = rec.getKey("documentkey");
		assert key != null;
		deleteRecord(key);
	}
	
	void deleteRecord(Key key) throws SQLException {
		logger.trace(Log.PROCESS, "Delete " + key);		
		if (deleteStmt.deleteRecord(key))
			writerMetrics.incrementDeleted();
		else
			writerMetrics.incrementSkipped();		
	}

}
