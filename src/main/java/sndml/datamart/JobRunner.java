package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public abstract class JobRunner implements Callable<WriterMetrics> {

	protected final Session session;
	protected final Database db;
	protected final JobConfig config;
	protected Table table;
	protected String sqlTableName;
	protected String tableLoaderName;
	protected WriterMetrics writerMetrics;
	protected Key runKey;
	protected AppRunLogger appRunLogger;		
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	JobRunner(Session session, Database db, JobConfig config) {
		this.session = session;
		this.db = db;
		this.config = config;		
	}
	
	private void setLogContext() {
		Log.setContext(table, tableLoaderName);		
	}
		
	String getName() {
		return this.table.getName();
	}
	
	JobConfig getConfig() {
		return this.config;
	}

	@Deprecated
	WriterMetrics getMetrics() {
		return writerMetrics;
	}
	
	protected void close() throws ResourceException {		
	}
	
	@Override
	public WriterMetrics call() throws SQLException, IOException, InterruptedException {
		assert config != null;
		Action action = config.getAction();
		assert action != null;		
		this.setLogContext();
		logger.debug(Log.INIT, 
			String.format("call table=%s action=%s", table.getName(), action.toString()));
		if (config.getSqlBefore() != null) runSQL(config.getSqlBefore());
		this.setLogContext();
		WriterMetrics writerMetrics = null;
		switch (action) {
		case CREATE:
			runCreate();
			break;
		case DROPTABLE:
			db.dropTable(sqlTableName, true);
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
			logger.info(Log.FINISH, String.format("end load %s (%d rows)", tableLoaderName, processed));
			Integer minRows = config.getMinRows();
			if (minRows != null && processed < minRows)
				throw new TooFewRowsException(table, minRows, processed);			
		}
		if (config.getSqlAfter() != null) runSQL(config.getSqlAfter());
		this.close();
		this.writerMetrics = writerMetrics;
		return writerMetrics;		
	}
	
	void runSQL(String sqlCommand) throws SQLException {		
		db.executeStatement(sqlCommand);
		db.commit();
	}
	
	void runCreate() throws SQLException, IOException, InterruptedException {
		assert sqlTableName != null;
		assert sqlTableName.length() > 0;
		if (config.getDropTable()) db.dropTable(sqlTableName, true);
		db.createMissingTable(table, sqlTableName);		
	}
	
	WriterMetrics runPrune() throws SQLException, IOException, InterruptedException {
		assert sqlTableName != null;
		assert sqlTableName.length() > 0;
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
		ProgressLogger progressLogger = 
				new CompositeProgressLogger(auditReader, appRunLogger);
		deleteWriter.setProgressLogger(progressLogger);
		deleteWriter.open();
		auditReader.setWriter(deleteWriter);		
		auditReader.initialize();
		this.setLogContext();
		logger.info(Log.INIT, String.format("begin delete %s (%d rows)", 
			tableLoaderName, auditReader.getReaderMetrics().getExpected()));
		auditReader.call();
		deleteWriter.close();
		return deleteWriter.getMetrics();
	}
	
	WriterMetrics runSync() throws SQLException, IOException, InterruptedException {
		assert sqlTableName != null;
		assert sqlTableName.length() > 0;
		DateTimeRange createdRange = config.getCreated();
		ProgressLogger progressLogger;
		if (config.getAutoCreate()) 
			db.createMissingTable(table, sqlTableName);
		Interval partitionInterval = config.getPartitionInterval();
		if (partitionInterval == null) {
			Synchronizer syncReader = new Synchronizer(table, db, sqlTableName);
			progressLogger = new CompositeProgressLogger(syncReader, appRunLogger);
			syncReader.setProgressLogger(progressLogger);
			syncReader.setFields(config.getColumns(table));
			syncReader.setPageSize(config.getPageSize());
			syncReader.initialize(createdRange);
			logger.info(Log.INIT, String.format("begin sync %s (%d rows)", 
					tableLoaderName, syncReader.getReaderMetrics().getExpected()));
			syncReader.call();
			return syncReader.getWriterMetrics();
		}
		else {
			SynchronizerFactory factory = 
				new SynchronizerFactory(table, db, sqlTableName, createdRange, appRunLogger);
			factory.setFields(config.getColumns(table));
			factory.setPageSize(config.getPageSize());
			DatePartitionedTableReader multiReader =
				new DatePartitionedTableReader(factory, partitionInterval, config.getThreads());
			factory.setParent(multiReader);				
			multiReader.initialize();
			this.setLogContext();
			logger.info(Log.INIT, "partition=" + multiReader.getPartition());
			logger.info(Log.INIT, String.format("begin sync %s (%d rows)", 
					tableLoaderName, multiReader.getReaderMetrics().getExpected()));
			multiReader.call();
			return multiReader.getWriterMetrics();
		}
	}
	
	WriterMetrics runLoad() throws SQLException, IOException, InterruptedException {
		assert sqlTableName != null;
		assert sqlTableName.length() > 0;
		if (this instanceof DaemonJobRunner) {
			assert appRunLogger != null;
		}
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
//		writer.setParentMetrics(metrics);
		writer.open();
		
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
		factory.setReaderName(tableLoaderName);
		factory.setFilter(new EncodedQuery(table, config.getFilter()));
		factory.setCreated(config.getCreated());
		factory.setFields(config.getColumns(table));
		factory.setPageSize(config.getPageSize());
		TableReader reader;
		ProgressLogger progressLogger;
		if (partitionInterval == null) {
			reader = factory.createReader();
			reader.setMaxRows(config.getMaxRows());
			progressLogger = new CompositeProgressLogger(reader, appRunLogger);
			reader.setProgressLogger(progressLogger);
			Log.setContext(table, tableLoaderName);
			if (since != null) logger.info(Log.INIT, "getKeys " + reader.getQuery().toString());
			reader.initialize();
		}
		else {
			Integer threads = config.getThreads();
			DatePartitionedTableReader multiReader = 
					new DatePartitionedTableReader(factory, partitionInterval, threads);
			reader = multiReader;
			factory.setParent(multiReader);
			multiReader.initialize();
			Log.setContext(table, tableLoaderName);
			logger.info(Log.INIT, "partition=" + multiReader.getPartition());
		}
		logger.info(Log.INIT, String.format("begin load %s (%d rows)", 
				tableLoaderName, reader.getReaderMetrics().getExpected()));
		reader.call();
		writer.close();	
		return reader.getWriterMetrics();
	}

}
