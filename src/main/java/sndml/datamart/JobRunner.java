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
	protected Metrics writerMetrics;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	JobRunner(Session session, Database db, JobConfig config) {
		this.session = session;
		this.db = db;
		this.config = config;		
	}
			
	String getName() {
		return config.getName();
	}
	
	JobConfig getConfig() {
		return config;
	}
	
	protected ProgressLogger newProgressLogger(TableReader reader) {
		ProgressLogger progressLogger = new Log4jProgressLogger(reader, action);
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
		writerMetrics = new Metrics(config.getName(), null);
		writerMetrics.start();
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
		writerMetrics.finish();
		int processed = writerMetrics.getProcessed();
		Integer minRows = config.getMinRows();
		if (minRows != null && processed < minRows)
			throw new TooFewRowsException(table, minRows, processed);			
		if (config.getSqlAfter() != null) runSQL(config.getSqlAfter());
		return writerMetrics;
	}
	
	private void runSQL(String sqlCommand) throws SQLException {
		writerMetrics.start();
		db.executeStatement(sqlCommand);
		db.commit();
	}
	
	private void runCreateTable() throws SQLException, IOException, InterruptedException {
		logger.debug(Log.INIT, "runCreateTable " + config.getTarget());
		writerMetrics.start();
		String sqlTableName = config.getTarget();
		assert table != null;
		assert sqlTableName != null;
		if (config.getDropTable()) db.dropTable(sqlTableName, true);
		db.createMissingTable(table, sqlTableName, config.getColumns());		
	}
	
	private void runDropTable() throws SQLException {
		logger.debug(Log.INIT, "runDropTable " + config.getTarget());
		writerMetrics.start();
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
		ProgressLogger progressLogger = newProgressLogger(auditReader);
		deleteWriter.open(writerMetrics);
		auditReader.setWriter(deleteWriter, writerMetrics);
		auditReader.setProgressLogger(progressLogger);
		auditReader.initialize();
		Log.setTableContext(table, config.getName());
		auditReader.call();
		deleteWriter.close(writerMetrics);
	}
	
	private void runSync() throws SQLException, IOException, InterruptedException {
		String sqlTableName = config.getTarget();
		assert sqlTableName != null;
		if (config.getAutoCreate()) 
			db.createMissingTable(table, sqlTableName, config.getColumns());
		Interval partitionInterval = config.getPartitionInterval();
		TableReaderFactory factory = new TableReaderFactory(table, db, config);
		TableReader reader;
		if (partitionInterval == null) {
			reader = factory.createReader(null);
			ProgressLogger progressLogger = newProgressLogger(reader);
			reader.setProgressLogger(progressLogger);
			reader.setFields(config.getColumns());
			reader.setPageSize(config.getPageSize());
			reader.initialize();
			writerMetrics = reader.call();
		}
		else {
			DatePartitionedTableReader multiReader = 
				new DatePartitionedTableReader(
					factory, config.getName(), partitionInterval, config.getThreads());
			ProgressLogger progressLogger = newProgressLogger(multiReader);
			multiReader.setProgressLogger(progressLogger);
			multiReader.initialize();
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
		TableReaderFactory factory = new TableReaderFactory(table, db, config);
		factory.setWriter(writer, writerMetrics);
		ProgressLogger progressLogger;
		if (partitionInterval == null) {
			TableReader reader = factory.createReader(null);
			factory.setParentReader(reader);
			reader.setMaxRows(config.getMaxRows());
			assert reader.getReaderName() != null;
			assert reader.getMetrics().getName() != null;
			progressLogger = newProgressLogger(reader);
			writer.open(writerMetrics);
			Log.setTableContext(table, config.getName());					
			if (since != null) logger.info(Log.INIT, "getKeys " + reader.getQuery().toString());
			reader.setProgressLogger(progressLogger);
			reader.initialize();
			reader.call();
		}
		else {
			Integer threads = config.getThreads();
			DatePartitionedTableReader multiReader = 
				new DatePartitionedTableReader(
					factory, config.getName(), partitionInterval, threads);
			factory.setParentReader(multiReader);
			progressLogger = newProgressLogger(multiReader);
			multiReader.setProgressLogger(progressLogger);
			assert multiReader.getReaderName() != null;
			writer.open(writerMetrics);
			multiReader.initialize();
			DatePartition partition = multiReader.getPartition();
			logger.info(Log.INIT, "partition=" + partition.toString());
			Log.setTableContext(table, config.getName());
			multiReader.call();
		}
		writer.close(writerMetrics);
	}

}
