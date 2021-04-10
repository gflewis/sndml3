package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.Metrics;
import sndml.servicenow.BaseRecord;
import sndml.servicenow.Table;

@Deprecated
public class DatabaseSyncWriter extends DatabaseTableWriter {

	private final DatabaseInsertWriter insertWriter;
	private final DatabaseUpdateWriter updateWriter;
	private final DatabaseDeleteWriter deleteWriter;
	private final Metrics metrics;
	
	public DatabaseSyncWriter(Database db, Table table, String sqlTableName, 
			String writerName, Metrics metrics)
			throws IOException, SQLException {
		super(db, table, sqlTableName, writerName);
		insertWriter = new DatabaseInsertWriter(db, table, sqlTableName, writerName + ".INSERT");
		updateWriter = new DatabaseUpdateWriter(db, table, sqlTableName, writerName + ".UPDATE");
		deleteWriter = new DatabaseDeleteWriter(db, table, sqlTableName, writerName + ".DELETE");
		this.metrics = metrics;
	}

	@Override 
	public DatabaseSyncWriter open(Metrics metrics) throws SQLException, IOException {
		super.open(metrics);
		insertWriter.open(metrics);
		updateWriter.open(metrics);
		deleteWriter.open(metrics);
		return this;
	}
	
	@Override
	void writeRecord(BaseRecord rec, Metrics metrics) throws SQLException {
		throw new UnsupportedOperationException();

	}
	
	void insertRecord(BaseRecord rec) throws SQLException {
		insertWriter.writeRecord(rec, metrics);
	}
	
	void updateRecord(BaseRecord rec) throws SQLException {
		updateWriter.writeRecord(rec, metrics);;
	}
	
	void deleteRecord(BaseRecord rec) throws SQLException {
		deleteWriter.writeRecord(rec, metrics);
	}
	

}
