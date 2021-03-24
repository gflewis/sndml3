package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class JobRunner implements Callable<Metrics> {

	protected final Session session;
	protected final Database db;
	protected final JobConfig config;
	
	protected Action action;
	protected Table table;
	protected Metrics jobMetrics;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public JobRunner(Session session, Database db, JobConfig config) {
		this.session = session;
		this.db = db;
		this.config = config;		
	}
			
	protected String getName() {
		return config.getName();
	}
	
	protected JobConfig getConfig() {
		return config;
	}
	
	protected ProgressLogger createJobProgressLogger(TableReader reader) {
		ProgressLogger progressLogger = 
			new Log4jProgressLogger(reader.getClass(), action, jobMetrics);
		reader.setMetrics(jobMetrics);
		reader.setProgressLogger(progressLogger);
		return progressLogger;
	}
			
	protected void close() throws ResourceException {
		// DaemonJobRunner will override this method
	}
	
	@Override
	public Metrics call() throws SQLException, IOException, InterruptedException {
		assert config != null;
		action = config.getAction();
		assert action != null;
		if (Action.EXECUTE_ONLY.contains(action)) {
			// Action with no table
			table = null;
			Log.setJobContext(config.getName());
		}
		else {
			// Action with a source table
			table = session.table(config.getSource());
			Log.setTableContext(table, config.getName());		
			logger.debug(Log.INIT, String.format(
				"call table=%s action=%s", 
				table.getName(), action.toString()));
		}
		jobMetrics = new Metrics(config.getName(), null);
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
	
	private void runSQL(String sqlCommand) throws SQLException {
		db.executeStatement(sqlCommand);
		db.commit();
	}
	
	private void runCreateTable() throws SQLException, IOException, InterruptedException {
		logger.debug(Log.INIT, "runCreateTable " + config.getTarget());
		String sqlTableName = config.getTarget();
		assert table != null;
		assert sqlTableName != null;
		if (config.getDropTable()) db.dropTable(sqlTableName, true);
		db.createMissingTable(table, sqlTableName, config.getColumns());		
	}
	
	private void runDropTable() throws SQLException {
		logger.debug(Log.INIT, "runDropTable " + config.getTarget());
		jobMetrics.start();
		db.dropTable(config.getTarget(), true);
	}
	
	private void runPrune() throws SQLException, IOException, InterruptedException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		Table audit = session.table("sys_audit_delete");
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
			new DatabaseDeleteWriter(db, table, sqlTableName, config.getName());
		ProgressLogger progressLogger = createJobProgressLogger(auditReader);
		deleteWriter.open(jobMetrics);
//		auditReader.setWriter(deleteWriter, jobMetrics);
		auditReader.prepare(deleteWriter, jobMetrics, progressLogger);
		Log.setTableContext(table, config.getName());
		auditReader.call();
		deleteWriter.close(jobMetrics);
	}
	
	private void runSync() throws SQLException, IOException, InterruptedException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		if (config.getAutoCreate()) 
			db.createMissingTable(table, sqlTableName, config.getColumns());
		Interval partitionInterval = config.getPartitionInterval();
		TableReader reader;
		if (partitionInterval == null) {
			reader = config.createReader(table, db, null);			
			ProgressLogger progressLogger = createJobProgressLogger(reader);
			reader.setFields(config.getColumns());
			reader.setPageSize(config.getPageSize());
			reader.prepare(null, jobMetrics, progressLogger);
			jobMetrics = reader.call();
		}
		else {
			DatePartitionedTableReader multiReader = 
				new DatePartitionedTableReader(table, config, db);
			ProgressLogger progressLogger = createJobProgressLogger(multiReader);	
			multiReader.setMetrics(jobMetrics);;
			assert multiReader.getMetrics() == jobMetrics;
			multiReader.prepare(null, jobMetrics, progressLogger);
			DatePartition partition = multiReader.getPartition();
			logger.info(Log.INIT, "partition=" + partition.toString());
			Log.setTableContext(table, config.getName());
			multiReader.call();
		}
	}
	
	private void runLoad() throws SQLException, IOException, InterruptedException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		Action action = config.getAction();		
		if (config.getAutoCreate()) 
			db.createMissingTable(table, sqlTableName, config.getColumns());
		if (config.getTruncate()) db.truncateTable(sqlTableName);
		
		DatabaseTableWriter writer;
		if (Action.INSERT.equals(action) || Action.LOAD.equals(action)) {
			writer = new DatabaseInsertWriter(db, table, sqlTableName, config.getName());
		}
		else {
			writer = new DatabaseUpdateWriter(db, table, sqlTableName, config.getName());
		}
		Interval partitionInterval = config.getPartitionInterval();
		DateTime since = config.getSince();	
		logger.debug(Log.INIT, "since=" + config.sinceExpr + "=" + since);
		if (partitionInterval == null) {
			TableReader reader = config.createReader(table, db, null);
			ProgressLogger progressLogger = createJobProgressLogger(reader);
//			reader.setWriter(writer, jobMetrics);
			writer.open(jobMetrics);
			assert reader.getMetrics().getName() == config.getName();
			Log.setTableContext(table, config.getName());					
			if (since != null) logger.info(Log.INIT, "getKeys " + reader.getQuery().toString());
			reader.prepare(writer, jobMetrics, progressLogger);
			reader.call();
		}
		else {
			DatePartitionedTableReader multiReader = 
				new DatePartitionedTableReader(table, config, db);
			ProgressLogger progressLogger = createJobProgressLogger(multiReader);
//			multiReader.setWriter(writer, jobMetrics);
			writer.open(jobMetrics);
			multiReader.prepare(writer, jobMetrics, progressLogger);
			DatePartition partition = multiReader.getPartition();
			logger.info(Log.INIT, "partition=" + partition.toString());
			Log.setTableContext(table, config.getName());
			multiReader.call();
		}
		writer.close(jobMetrics);
	}

}
