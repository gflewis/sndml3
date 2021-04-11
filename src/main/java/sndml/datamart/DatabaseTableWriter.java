package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;

import sndml.servicenow.Log;
import sndml.servicenow.Metrics;
import sndml.servicenow.ProgressLogger;
import sndml.servicenow.TableRecord;
import sndml.servicenow.RecordList;
import sndml.servicenow.RecordWriter;
import sndml.servicenow.Table;

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
		super();
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
		super.open(metrics);
		columns = new ColumnDefinitions(this.db, this.table, this.sqlTableName);
		metrics.start();
		return this;
	}
	
	@Override
	public void close(Metrics metrics) {
		try {
			db.commit();
		} catch (SQLException e) {
			throw new ResourceException(e);
		}
		metrics.finish();
		super.close(metrics);
	}

	@Override
	public synchronized void processRecords(
			RecordList recs, Metrics metrics, ProgressLogger progressLogger) 
			throws IOException, SQLException {
		assert metrics != null;
		assert progressLogger != null;
		for (TableRecord rec : recs) {
			logger.debug(Log.PROCESS, String.format(
				"processing %s %s", rec.getCreatedTimestamp(), rec.getKey()));
			writeRecord(rec, metrics);
		}
		db.commit();
		progressLogger.logProgress();
	}
	
	abstract void writeRecord(TableRecord rec, Metrics writerMetrics) throws SQLException;
	
}
