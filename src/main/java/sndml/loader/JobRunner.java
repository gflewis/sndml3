package sndml.loader;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.agent.JobCancelledException;
import sndml.servicenow.*;
import sndml.util.DateTime;
import sndml.util.DateTimeRange;
import sndml.util.Log;
import sndml.util.Metrics;
import sndml.util.ResourceException;

public class JobRunner implements Callable<Metrics> {

	protected final JobConfig config;
	
	protected final Session readerSession;
	protected final DatabaseWrapper database;
	
	protected Action action;
	protected Table table;
	protected Metrics jobMetrics;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
		
	public JobRunner(Session readerSession, DatabaseWrapper db, JobConfig config) {
		this.readerSession = readerSession;
		this.database = db;
		this.config = config;		
		assert readerSession!= null;
		assert db != null;
		assert config != null;
	}
			
	protected String getName() {
		return config.getName();
	}
	
	protected JobConfig getConfig() {
		return config;
	}
	
	protected ProgressLogger createJobProgressLogger(TableReader reader) {
		ProgressLogger progressLogger;
		if (reader == null) {
			progressLogger = new Log4jProgressLogger(this.getClass(), action, jobMetrics);			
		}
		else {
			progressLogger = new Log4jProgressLogger(reader.getClass(), action, jobMetrics);
		}
		return progressLogger;
	}

	/**
	 * Override this method to release all resources.
	 * {@Link AppJobRunner} will override this method
	 * @throws ResourceException
	 */
	public void close() throws ResourceException {
	}

	
	/**
	 * This method calls one of several different private "run" functions based on the action. 
	 * The general format of each of these functions is as follows:
	 * <ol>
	 * <li>TableReader reader = config.createReader(table, database); </li>
	 * <li>DatabaseTableWriter writer = new DatabaseTableWriter(); </li>
	 * <li>ProgressLogger progressLogger = createJobProgressLogger(reader); </li>
	 * <li>writer.open(jobMetrics); </li>
	 * <li>reader.prepare(writer, jobMetrics, progressLogger</li>
	 * <li>reader.call(); </li>
	 * <li>writer.close(jobMetrics)</li>
	 * </ol>
	 */
	@Override
	public Metrics call() throws SQLException, IOException, InterruptedException, JobCancelledException {
		assert config != null;
		assert readerSession != null;
		assert database != null;
		action = config.getAction();
		assert action != null;
		Log.setJobContext(config.getName());
		if (Action.EXECUTE_ONLY.contains(action)) {
			// Action with no table
			table = null;
		}
		else {
			// Action with a source table
			table = readerSession.table(config.getSource());
			logger.debug(Log.INIT, String.format(
				"call table=%s action=%s", 
				table.getName(), action.toString()));
		}
		jobMetrics = new Metrics(config.getName());
		jobMetrics.start();
		if (config.getSqlBefore() != null) runSQL(config.getSqlBefore());
		switch (action) {
		case CREATE:
			runCreateTable();
			break;
		case DROPTABLE:
			runDropTable();
			break;
		case EXECUTE:
			runSQL(config.getSql());
			break;
		case PRUNE:
			runPrune();
			break;
		case SYNC:
			runSync();
			break;
		case SINGLE:
			runSingleRecordSync();
			break;
		default:
			runLoad();
		}
		jobMetrics.finish();
		int processed = jobMetrics.getProcessed();
		Integer minRows = config.getMinRows();
		if (minRows != null && processed < minRows)
			throw new TooFewRowsException(table, minRows, processed);			
		if (config.getSqlAfter() != null) runSQL(config.getSqlAfter());
		return jobMetrics;
	}
	
	private void runSQL(String sqlCommand) throws SQLException, JobCancelledException {
		logger.debug(Log.INIT, "runSQL " + sqlCommand);		
		jobMetrics.setExpected(0);
		ProgressLogger progressLogger = createJobProgressLogger(null);
		progressLogger.logStart();
		database.executeStatement(sqlCommand);
		database.commit();
		progressLogger.logComplete();
	}
	
	private void runCreateTable() throws SQLException, IOException, InterruptedException {
		logger.debug(Log.INIT, "runCreateTable " + config.getTarget());		
		String sqlTableName = config.getTarget();
		assert table != null;
		assert sqlTableName != null;
		jobMetrics.setExpected(0);
		ProgressLogger progressLogger = createJobProgressLogger(null);
		if (config.getDropTable()) database.dropTable(sqlTableName, true);
		database.createMissingTable(table, sqlTableName, config.getColumns());
		progressLogger.logComplete();
	}
	
	private void runDropTable() throws SQLException {
		logger.debug(Log.INIT, "runDropTable " + config.getTarget());
		jobMetrics.start();
		database.dropTable(config.getTarget(), true);
	}
	
	private void runPrune() throws SQLException, IOException, InterruptedException, JobCancelledException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		Table audit = readerSession.table("sys_audit_delete");
		EncodedQuery auditQuery = new EncodedQuery(audit);
		auditQuery.addQuery("tablename", EncodedQuery.EQUALS, table.getName());
		RestTableReader auditReader = new RestTableReader(audit);
		auditReader.enableStats(true);
		auditReader.orderByKeys(true);
		auditReader.setFilter(auditQuery);			
		DateTime since = config.getSince();
		auditReader.setCreatedRange(new DateTimeRange(since, null));
		auditReader.setMaxRows(config.getMaxRows());
		DatabaseDeleteWriter deleteWriter = 
			new DatabaseDeleteWriter(database, table, sqlTableName, config.getName());
		ProgressLogger progressLogger = createJobProgressLogger(auditReader);
		deleteWriter.open(jobMetrics);
		auditReader.prepare(deleteWriter, jobMetrics, progressLogger);
		Log.setTableContext(table, config.getName());
		auditReader.call();
		deleteWriter.close(jobMetrics);
	}
	
	private void runSync() throws SQLException, IOException, InterruptedException, JobCancelledException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		logger.debug(Log.INIT, "runSync " + config.toString());
		if (config.getAutoCreate()) 
			database.createMissingTable(table, sqlTableName, config.getColumns());
		Interval partitionInterval = config.getPartitionInterval();
		TableReader synchronizer;
		if (partitionInterval == null) {
			synchronizer = config.createReader(table, database);			
			ProgressLogger progressLogger = createJobProgressLogger(synchronizer);
			synchronizer.prepare(null, jobMetrics, progressLogger);
		}
		else {
			DatePartitionedTableReader multiReader = 
				new DatePartitionedTableReader(table, config, database);
			synchronizer = multiReader;
			ProgressLogger progressLogger = createJobProgressLogger(multiReader);	
			synchronizer.prepare(null, jobMetrics, progressLogger);
			DatePartition partition = multiReader.getPartition();
			logger.info(Log.INIT, "partition=" + partition.toString());
		}
		assert(synchronizer instanceof TableSynchronizer);
		Log.setTableContext(table, config.getName());
		synchronizer.call();
	}
	
	private void runSingleRecordSync() 
			throws SQLException, IOException, InterruptedException, JobCancelledException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		RecordKey recordKey = config.getDocKey();
		logger.debug(Log.INIT, "runSingle " + config.toString());
		if (config.getAutoCreate()) 
			database.createMissingTable(table, sqlTableName, config.getColumns());
		TableReader reader = new RestPetitTableReader(table);
		config.configureReader(reader);
		reader.setPageSize(100); // page size needs to be at least 2
		Log.setTableContext(table, config.getName());					
				DatabaseRecordSyncWriter writer = 
			new DatabaseRecordSyncWriter(database, table, sqlTableName, recordKey, config.getName());				
		ProgressLogger progressLogger = createJobProgressLogger(reader);		
		writer.open(jobMetrics);
		reader.prepare(writer, jobMetrics, progressLogger);
		Log.setTableContext(table, config.getName());
		assert reader.getMetrics() != null;
		reader.call();
		writer.close(jobMetrics);
	}
	
	private void runLoad() throws SQLException, IOException, InterruptedException, JobCancelledException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		Action action = config.getAction();	
		logger.debug(Log.INIT, "runLoad " + config.toString());
		if (config.getAutoCreate()) 
			database.createMissingTable(table, sqlTableName, config.getColumns());
		if (config.getTruncate()) database.truncateTable(sqlTableName);
		
		DatabaseTableWriter writer;
		if (Action.INSERT.equals(action) || Action.LOAD.equals(action)) {
			writer = new DatabaseInsertWriter(database, table, sqlTableName, config.getName());
		}
		else {
			writer = new DatabaseUpdateWriter(database, table, sqlTableName, config.getName());
		}
		writer.open(jobMetrics);
		Interval partitionInterval = config.getPartitionInterval();
		DateTime since = config.getSince();	
		logger.debug(Log.INIT, "since=" + config.sinceExpr + "=" + since);
		TableReader reader;
		Log.setTableContext(table, config.getName());					
		if (partitionInterval == null) {
			reader = config.createReader(table, database);
			ProgressLogger progressLogger = createJobProgressLogger(reader);
			if (since != null) logger.info(Log.INIT, "getKeys " + reader.getQuery().toString());
			reader.prepare(writer, jobMetrics, progressLogger);
		}
		else {
			DatePartitionedTableReader multiReader = new DatePartitionedTableReader(table, config, database);
			reader = multiReader;
			ProgressLogger progressLogger = createJobProgressLogger(multiReader);
			reader.prepare(writer, jobMetrics, progressLogger);
			DatePartition partition = multiReader.getPartition();
			logger.info(Log.INIT, "partition=" + partition.toString());
		}
		assert reader.getMetrics() != null;
		assert reader.getMetrics().getName() == config.getName();
		Log.setTableContext(table, config.getName());
		JobCancelledException cancel = null;
		try {
			reader.call();
		} catch (JobCancelledException e) {
			cancel = e;
		}
		writer.close(jobMetrics);
		if (cancel != null) throw(cancel);
	}

}
