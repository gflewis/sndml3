package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class JobRunner implements Callable<JobRunner> {

	protected final Session session;
	protected final Database db;
	protected final JobConfig config;
	
	protected Action action;
	protected Table table;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	private WriterMetrics writerMetrics = null;
	
	JobRunner(Session session, Database db, JobConfig config) {
		this.session = session;
		this.db = db;
		this.config = config;		
	}
			
	String getName() {
		return this.table.getName();
	}
	
	JobConfig getConfig() {
		return this.config;
	}

	WriterMetrics getWriterMetrics() {
		assert writerMetrics != null : "Not yet called";
		return writerMetrics;
	}
	
	protected ProgressLogger newProgressLogger(TableReader reader) {
		ProgressLogger progressLogger = new Log4jProgressLogger(reader);
		reader.setProgressLogger(progressLogger);
		return progressLogger;
	}
	
	protected void close() throws ResourceException {		
	}
	
	@Override
	public JobRunner call() throws SQLException, IOException, InterruptedException {
		assert config != null;
		action = config.getAction();
		assert action != null;
		if (Action.anyGlobalAction.contains(action)) {
			table = null;
			Log.setJobContext(config.getName());
		}
		else {
			table = session.table(config.getSource());
			Log.setTableContext(table, config.getName());		
			logger.debug(Log.INIT, String.format(
				"call table=%s action=%s", 
				table.getName(), action.toString()));
		}
		if (writerMetrics != null) throw new IllegalStateException("Already called");
		if (config.getSqlBefore() != null) runSQL(config.getSqlBefore());
		WriterMetrics writerMetrics = null;
		switch (action) {
		case CREATE:
			runCreate();
			break;
		case DROPTABLE:
			db.dropTable(config.getTarget(), true);
			break;
		case EXECUTE:
			runSQL(config.getSql());
			break;
		case PRUNE:
			writerMetrics = runPrune();
			break;
		case SYNC:
			writerMetrics = runSync();
			break;
		default:
			writerMetrics = runLoad();
		}
		if (writerMetrics != null) {
			int processed = writerMetrics.getProcessed();
			logger.info(Log.FINISH, String.format("end load %s (%d rows)", 
					config.getName(), processed));
			Integer minRows = config.getMinRows();
			if (minRows != null && processed < minRows)
				throw new TooFewRowsException(table, minRows, processed);			
		}
		if (config.getSqlAfter() != null) runSQL(config.getSqlAfter());
		this.close();
		this.writerMetrics = writerMetrics;
		return this;
	}
	
	void runSQL(String sqlCommand) throws SQLException {		
		db.executeStatement(sqlCommand);
		db.commit();
	}
	
	void runCreate() throws SQLException, IOException, InterruptedException {
		String sqlTableName = config.getTarget();
		assert table != null;
		assert sqlTableName != null;
		if (config.getDropTable()) db.dropTable(sqlTableName, true);
		db.createMissingTable(table, sqlTableName);		
	}
	
	WriterMetrics runPrune() throws SQLException, IOException, InterruptedException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		Table audit = session.table("sys_audit_delete");
		EncodedQuery auditQuery = new EncodedQuery(audit);
		auditQuery.addQuery("tablename", EncodedQuery.EQUALS, table.getName());
		RestTableReader auditReader = new RestTableReader(audit);
		auditReader.enableStats(true);
		auditReader.orderByKeys(true);
		auditReader.setQuery(auditQuery);			
		DateTime since = config.getSince();
		auditReader.setCreatedRange(new DateTimeRange(since, null));
		auditReader.setMaxRows(config.getMaxRows());
		DatabaseDeleteWriter deleteWriter = 
			new DatabaseDeleteWriter(db, table, sqlTableName);
		ProgressLogger progressLogger = newProgressLogger(auditReader);
		deleteWriter.setProgressLogger(progressLogger);
		deleteWriter.open();
		auditReader.setWriter(deleteWriter);		
		auditReader.initialize();
		Log.setTableContext(table, config.getName());
		progressLogger.logStart(auditReader, "delete");
		auditReader.call();
		deleteWriter.close();
		return deleteWriter.getWriterMetrics();
	}
	
	WriterMetrics runSync() throws SQLException, IOException, InterruptedException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		DateTimeRange createdRange = config.getCreated();
		if (config.getAutoCreate()) 
			db.createMissingTable(table, sqlTableName);
		Interval partitionInterval = config.getPartitionInterval();
		if (partitionInterval == null) {
			Synchronizer syncReader = new Synchronizer(table, db, sqlTableName);
			ProgressLogger progressLogger = newProgressLogger(syncReader);
			progressLogger.setOperation("sync");
			syncReader.setFields(config.getColumns(table));
			syncReader.setPageSize(config.getPageSize());
			syncReader.initialize(createdRange);
			progressLogger.logStart(syncReader, "sync");
			syncReader.call();
			progressLogger.logFinish(syncReader);
			return syncReader.getWriterMetrics();
		}
		else {
			SynchronizerFactory factory = 
				new SynchronizerFactory(table, db, sqlTableName, createdRange);
			factory.setFields(config.getColumns(table));
			factory.setPageSize(config.getPageSize());
			DatePartitionedTableReader multiReader =
				new DatePartitionedTableReader(factory, partitionInterval, config.getThreads());
			DatePartition partition = multiReader.getPartition();
			logger.info(Log.INIT, "partition=" + partition.toString());
			factory.setParent(multiReader);				
			ProgressLogger progressLogger = newProgressLogger(multiReader);
			multiReader.initialize();
			Log.setTableContext(table, config.getName());
			progressLogger.logStart(multiReader, "sync");
			multiReader.call();
			progressLogger.logFinish(multiReader);
			return multiReader.getWriterMetrics();
		}
	}
	
	WriterMetrics runLoad() throws SQLException, IOException, InterruptedException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		Action action = config.getAction();		
		if (config.getAutoCreate()) db.createMissingTable(table, sqlTableName);
		if (config.getTruncate()) db.truncateTable(sqlTableName);
		
		DatabaseTableWriter writer;
		if (Action.INSERT.equals(action) || Action.LOAD.equals(action)) {
			writer = new DatabaseInsertWriter(db, table, sqlTableName);
		}
		else {
			writer = new DatabaseUpdateWriter(db, table, sqlTableName);
		}
		
		Interval partitionInterval = config.getPartitionInterval();
		TableReaderFactory factory;
		DateTime since = config.getSince();	
		logger.debug(Log.INIT, "since=" + config.sinceExpr + "=" + since);
		if (since != null) {
			factory = new KeySetTableReaderFactory(table, writer);
			factory.setUpdated(since);				
		}
		else {
			factory = new RestTableReaderFactory(table, writer);
		}
		factory.setReaderName(config.getName());
		factory.setFilter(new EncodedQuery(table, config.getFilter()));
		factory.setCreated(config.getCreated());
		factory.setFields(config.getColumns(table));
		factory.setPageSize(config.getPageSize());
		ProgressLogger progressLogger;
		WriterMetrics metrics;
		if (partitionInterval == null) {
			TableReader reader = factory.createReader();
			reader.setMaxRows(config.getMaxRows());
			progressLogger = newProgressLogger(reader);
			writer.setProgressLogger(progressLogger);
			writer.open();
			Log.setTableContext(table, config.getName());					
			if (since != null) logger.info(Log.INIT, "getKeys " + reader.getQuery().toString());
			reader.initialize();
			progressLogger.logStart(reader, "load");
			reader.call();
			progressLogger.logFinish(reader);
			metrics = reader.getWriterMetrics();
		}
		else {
			Integer threads = config.getThreads();
			DatePartitionedTableReader multiReader = 
				new DatePartitionedTableReader(factory, partitionInterval, threads);
			factory.setParent(multiReader);
			progressLogger = newProgressLogger(multiReader);
			multiReader.setProgressLogger(progressLogger);
			writer.setProgressLogger(progressLogger);
			writer.open();
			multiReader.initialize();
			DatePartition partition = multiReader.getPartition();
			logger.info(Log.INIT, "partition=" + partition.toString());
			Log.setTableContext(table, config.getName());
			progressLogger.logStart(multiReader, "load");
			multiReader.call();
			progressLogger.logFinish(multiReader);
			metrics = multiReader.getWriterMetrics();
		}
		writer.close();
		return metrics;
	}

}
