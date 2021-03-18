package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;

import sndml.servicenow.*;

/**
 * <p>A class which knows how to process records retrieved from ServiceNow.</p>
 * <p>There are three subclasses: one for each of the three database operations.</p>
 * <ul>
 * <li>{@link DatabaseInsertWriter}</li>
 * <li>{@link DatabaseUpdateWriter}</li>
 * <li>{@link DatabaseDeleteWriter}</li>
 * </ul>
 *
 */
public abstract class DatabaseTableWriter extends RecordWriter {

	final protected Database db;
	final protected Table table;
	final protected String sqlTableName;
	
	protected ColumnDefinitions columns;
	
	final Logger logger = Log.logger(this.getClass());
	
	public DatabaseTableWriter(Database db, Table table, String sqlTableName, String writerName) 
			throws IOException, SQLException {
		super(writerName);
		assert db != null;
		assert table != null;
		assert sqlTableName != null;
		this.db = db;
		this.table = table;
		this.sqlTableName = sqlTableName;
		Log.setTableContext(this.table);
	}
		
	@Override
	public DatabaseTableWriter open(Metrics metrics) throws SQLException, IOException {
		columns = new ColumnDefinitions(this.db, this.table, this.sqlTableName);
		metrics.start();
		return this;
	}
	
	@Override
	public void close(Metrics metrics) throws SQLException {
		db.commit();
		metrics.finish();
	}

	@Override
	public synchronized void processRecords(
			RecordList recs, Metrics metrics, ProgressLogger progressLogger) 
			throws IOException, SQLException {
		for (Record rec : recs) {
			logger.debug(Log.PROCESS, String.format(
				"processing %s %s", rec.getCreatedTimestamp(), rec.getKey()));
			writeRecord(rec, metrics);
		}
		db.commit();
		progressLogger.logProgress();
	}
	
	abstract void writeRecord(Record rec, Metrics writerMetrics) throws SQLException;
	
}
