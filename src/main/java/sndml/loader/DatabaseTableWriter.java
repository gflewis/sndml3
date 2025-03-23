package sndml.loader;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;

import sndml.agent.JobCancelledException;
import sndml.servicenow.TableRecord;
import sndml.util.Log;
import sndml.util.Metrics;
import sndml.util.ProgressLogger;
import sndml.util.ResourceException;
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

	final protected DatabaseWrapper db;
	final protected Table table;
	final protected String sqlTableName;
	
	protected ColumnDefinitions columns;
	
	final Logger logger = Log.getLogger(this.getClass());
	/**
	 * Abstract class which knows how to write records to a SQL database 
	 * using the {@link processRecords} method.
	 * Implementations must override the {@link writeRecord} method.
	 *  
	 * @param db Database connection
	 * @param table ServiceNow table
	 * @param sqlTableName name of the table in the SQL database
	 * @param writerName used only for logging
	 */
	public DatabaseTableWriter(DatabaseWrapper db, Table table, String sqlTableName, String writerName) 
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
		assert metrics != null;
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
			throws JobCancelledException, IOException, SQLException  {
		assert metrics != null;
		assert progressLogger != null;
		for (TableRecord rec : recs) {
			logger.trace(Log.PROCESS, String.format(
				"processing %s %s",  rec.getKey(), rec.getCreatedTimestamp()));
			writeRecord(rec, metrics);
		}
		db.commit();
		progressLogger.logProgress();
	}
	
	abstract void writeRecord(TableRecord rec, Metrics writerMetrics) throws SQLException;
	
}
