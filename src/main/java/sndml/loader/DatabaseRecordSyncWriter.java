package sndml.loader;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.TableRecord;
import sndml.util.DateTime;
import sndml.util.Log;
import sndml.util.Metrics;
import sndml.agent.JobCancelledException;
import sndml.servicenow.ProgressLogger;
import sndml.servicenow.RecordKey;
import sndml.servicenow.RecordList;
import sndml.servicenow.Table;

public class DatabaseRecordSyncWriter extends DatabaseTableWriter {

	private final RecordKey key;
	private final DatabaseInsertWriter insertWriter;
	private final DatabaseUpdateWriter updateWriter;
	private final DatabaseDeleteWriter deleteWriter;
	private final DatabaseTimestampReader dbtsr = new DatabaseTimestampReader(db);	
	private final Metrics metrics;
	
	public DatabaseRecordSyncWriter(DatabaseWrapper db, 
			Table table, String sqlTableName, 
			RecordKey key, String writerName)
			throws IOException, SQLException {
		super(db, table, sqlTableName, writerName);
		this.key = key;
		insertWriter = new DatabaseInsertWriter(db, table, sqlTableName, writerName + ".INSERT");
		updateWriter = new DatabaseUpdateWriter(db, table, sqlTableName, writerName + ".UPDATE");
		deleteWriter = new DatabaseDeleteWriter(db, table, sqlTableName, writerName + ".DELETE");
		this.metrics = new Metrics(writerName);
	}

	@Override 
	public DatabaseRecordSyncWriter open(Metrics metrics) throws SQLException, IOException {
		super.open(metrics);
		insertWriter.open(metrics);
		updateWriter.open(metrics);
		deleteWriter.open(metrics);
		return this;
	}

	@Override
	public synchronized void processRecords(
			RecordList recs, Metrics metrics, ProgressLogger progressLogger) 
			throws JobCancelledException, IOException, SQLException  {
		assert metrics != null;
		assert progressLogger != null;
		assert recs.size() < 2;
		DateTime databaseUpdated = dbtsr.getTimestampUpdated(sqlTableName,  key);
		boolean existsInDatabase = databaseUpdated == null ? false : true;
		if (recs.size() == 0 && existsInDatabase) {
			logger.info(Log.PROCESS, "deleting " + key.toString());
			deleteWriter.deleteRecord(key, metrics);
		}
		else {
			TableRecord rec = recs.get(0);
			if (existsInDatabase) { 
				logger.info(Log.PROCESS, "updating " + key.toString());
				updateWriter.writeRecord(rec, metrics);
			}
			else {
				logger.info(Log.PROCESS, "inserting " + key.toString());
				insertWriter.writeRecord(rec, metrics);
			}
		}
		db.commit();
		progressLogger.logProgress();
	}
	
	@Override
	void writeRecord(TableRecord rec, Metrics metrics) throws SQLException {		
		throw new UnsupportedOperationException();
	}
	
	void insertRecord(TableRecord rec) throws SQLException {
		insertWriter.writeRecord(rec, metrics);
	}
	
	void updateRecord(TableRecord rec) throws SQLException {
		updateWriter.writeRecord(rec, metrics);;
	}
	
	void deleteRecord(TableRecord rec) throws SQLException {
		deleteWriter.writeRecord(rec, metrics);
	}
	

}
