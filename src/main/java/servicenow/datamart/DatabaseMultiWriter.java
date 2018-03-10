package servicenow.datamart;

import servicenow.api.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Hashtable;

@Deprecated
public class DatabaseMultiWriter extends DatabaseTableWriter {

	final DateTimeRange createdRange;
	TimestampHash dbTimestamps;
	Hashtable<Key,Boolean> processedKeys;
	
	protected DatabaseInsertStatement insertStmt;
	protected DatabaseUpdateStatement updateStmt;
	protected DatabaseDeleteStatement deleteStmt;

	public DatabaseMultiWriter(Database db, Table table, String sqlTableName, DateTimeRange createdRange) 
			throws IOException, SQLException {
		super(db, table, sqlTableName);
		this.createdRange = createdRange;
	}
	
	@Override	
	public void open() throws SQLException, IOException {
		super.open();
		insertStmt = new DatabaseInsertStatement(this.db, this.sqlTableName, columns);
		updateStmt = new DatabaseUpdateStatement(this.db, this.sqlTableName, columns);
		deleteStmt = new DatabaseDeleteStatement(this.db, this.sqlTableName);		
		this.processedKeys = new Hashtable<Key,Boolean>(dbTimestamps.size());
		DatabaseTimestampReader tsr = new DatabaseTimestampReader(db);
		if (createdRange == null) 
			this.dbTimestamps = tsr.getTimestamps(sqlTableName);
		else
			this.dbTimestamps = tsr.getTimestamps(sqlTableName, createdRange);		
	}
	
	@Override
	void writeRecord(Record rec) throws SQLException {
		Key key = rec.getKey();
		DateTime updated = rec.getUpdatedTimestamp();
		DateTime dbUpdated = dbTimestamps.get(key);
		
		if (dbUpdated == null) {
			// Insert
			logger.trace(Log.PROCESS, "Insert " + key);
			insertStmt.insert(rec);
			writerMetrics.incrementInserted();
			processedKeys.put(key, true);
		}
		else {
			if (updated.equals(dbUpdated)) {				
				// Skip
				writerMetrics.incrementSkipped();
				processedKeys.put(key,  true);
			}
			else {
				// Update
				logger.trace(Log.PROCESS, "Update " + key);
				if (updateStmt.update(rec)) {
					writerMetrics.incrementUpdated();
					processedKeys.put(key, true);
				}
				else {
					throw new AssertionError("updaate failed");
				}				
			}
		}
		processedKeys.put(key,  true);
	}
	
	@Override
	public void close() throws SQLException {
		for (Key key : processedKeys.keySet()) {
			if (!processedKeys.contains(key))
				logger.trace(Log.PROCESS, "Delete " + key);
				deleteStmt.deleteRecord(key);			
		}
		super.close();
	}

}
