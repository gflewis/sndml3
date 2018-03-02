package servicenow.datamart;

import servicenow.api.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Hashtable;

import org.slf4j.Logger;


public class DatabaseSyncWriter extends DatabaseTableWriter {

	TableIndex dbEntries;
	
	Hashtable<Key,Boolean> processedKeys;
	
	protected DatabaseInsertStatement insertStmt;
	protected DatabaseUpdateStatement updateStmt;
	protected DatabaseDeleteStatement deleteStmt;

	final private Logger logger = Log.logger(this.getClass());

	public DatabaseSyncWriter(String name, Database db, Table table, String sqlTableName) throws IOException, SQLException {
		super(name, db, table, sqlTableName);
	}
	
	@Override
	public void open() {
		throw new UnsupportedOperationException();
	}
	
	public void open(TableIndex dbEntries) throws SQLException, IOException {
		super.open();
		insertStmt = new DatabaseInsertStatement(this.db, this.sqlTableName, columns);
		updateStmt = new DatabaseUpdateStatement(this.db, this.sqlTableName, columns);
		deleteStmt = new DatabaseDeleteStatement(this.db, this.sqlTableName);		
		this.dbEntries = dbEntries;
		this.processedKeys = new Hashtable<Key,Boolean>(dbEntries.size());
	}
	
	@Override
	void writeRecord(Record rec) throws SQLException {
		Key key = rec.getKey();
		DateTime created = rec.getCreatedTimestamp();
		DateTime updated = rec.getUpdatedTimestamp();
		TableIndex.Entry entry = dbEntries.get(key);
		if (entry == null) {
			// Insert
			logger.trace(Log.PROCESS, "Insert " + key);
			insertStmt.insert(rec);
			writerMetrics.incrementInserted();
			entry = dbEntries.add(key, created, updated);
			entry.processed = true;			
		}
		else {
			if (updated.equals(entry.updated)) {
				// Skip
				entry.processed = true;
			}
			else {
				// Update
				logger.trace(Log.PROCESS, "Update " + key);
				if (updateStmt.update(rec)) {
					writerMetrics.incrementUpdated();
					entry.processed = true;
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
		for (Key key : dbEntries.getKeySet()) {
			if (!processedKeys.contains(key))
				logger.trace(Log.PROCESS, "Delete " + key);
				deleteStmt.deleteRecord(key);			
		}
		super.close();
	}

}
