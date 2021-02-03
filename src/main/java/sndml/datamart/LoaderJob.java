package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class LoaderJob implements Callable<WriterMetrics> {

	private final Session session;
	private final Database db;
	private final Table table;
	private final String sqlTableName;
	private final String tableLoaderName;
	private final JobConfig config;
	private final WriterMetrics metrics;
	ProgressLogger progressLogger;
	
	
	Logger logger = LoggerFactory.getLogger(this.getClass());
		
	LoaderJob(Table table, Database database) throws ConfigParseException {
		this.config = new JobConfig(table);
		this.session = table.getSession();
		this.db = database;
		this.table = table;
		this.sqlTableName = config.getTargetName();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
	}
	
	LoaderJob(Loader parent, JobConfig config) throws ConfigParseException {
		config.validate();
		this.session = parent.getSession();
		this.db = parent.getDatabase();
		this.table = session.table(config.getSource());
		this.sqlTableName = config.getTargetName();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
		this.metrics.setParent(parent.getMetrics());
		this.config = config;
	}
	
	LoaderJob(ConnectionProfile profile, JobConfig config) {
		config.validate();
		this.session = profile.getSession();
		this.db = profile.getDatabase();
		this.table = session.table(config.getSource());
		this.sqlTableName = config.getTargetName();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
		this.config = config;		
	}
	
	LoaderJob(Session session, Database db, JobConfig config) {
		config.validate();
		this.session = session;
		this.db = db;
		this.table = session.table(config.getSource());
		this.sqlTableName = config.getTargetName();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
		this.config = config;				
	}

	private void setLogContext() {
		Log.setContext(table, tableLoaderName);		
	}
	
	public void setProgressLogger(ProgressLogger progress) {
		this.progressLogger = progress;
	}
	
	String getName() {
		return this.table.getName();
	}
	
	JobConfig getConfig() {
		return this.config;
	}

	WriterMetrics getMetrics() {
		return metrics;
	}

	public WriterMetrics call() throws SQLException, IOException, InterruptedException {
		assert sqlTableName != null;
		assert sqlTableName.length() > 0;
		JobAction action = config.getAction();
		assert action != null;
		DateTimeRange createdRange = config.getCreated();
		
		int pageSize = config.getPageSize() == null ? 0 : config.getPageSize().intValue();
		FieldNames fieldNames = null;
		if (config.getIncludeColumns() != null)
			fieldNames = config.getIncludeColumns();
		else if (config.getExcludeColumns() != null)
			fieldNames = table.getSchema().getFieldsMinus(config.getExcludeColumns());
		
		this.setLogContext();
		logger.debug(Log.INIT, 
			String.format("call table=%s action=%s", table.getName(), action.toString()));
		if (config.getSqlBefore() != null) {
			db.executeStatement(config.getSqlBefore());
		}		
		this.setLogContext();

		if (JobAction.CREATE.equals(action)) {
			if (config.getDropTable()) db.dropTable(sqlTableName, true);
			db.createMissingTable(table, sqlTableName);
		}
		else if (JobAction.PRUNE.equals(action)) {
			DatabaseDeleteWriter deleteWriter = new DatabaseDeleteWriter(db, table, sqlTableName);
			deleteWriter.setParentMetrics(metrics);
			deleteWriter.open();
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
			auditReader.setWriter(deleteWriter);
			auditReader.initialize();
			this.setLogContext();
			logger.info(Log.INIT, String.format("begin delete %s (%d rows)", 
				tableLoaderName, auditReader.getReaderMetrics().getExpected()));
			auditReader.call();
			deleteWriter.close();
		}
		else if (JobAction.SYNC.equals(action)) {
			db.createMissingTable(table, sqlTableName);
			DateTime.Interval partitionInterval = config.getPartitionInterval();
			TableReader reader;
			if (partitionInterval == null) {
				Synchronizer syncReader = new Synchronizer(table, db, sqlTableName, metrics);
				syncReader.setFields(fieldNames);
				syncReader.setPageSize(pageSize);
				syncReader.initialize(createdRange);
				reader = syncReader;
			}
			else {
				SynchronizerFactory factory = 
					new SynchronizerFactory(table, db, sqlTableName, this.metrics, createdRange);
				factory.setFields(fieldNames);
				factory.setPageSize(pageSize);
				DatePartitionedTableReader multiReader =
					new DatePartitionedTableReader(factory, partitionInterval, config.getThreads());
				factory.setParent(multiReader);				
				multiReader.initialize();
				reader = multiReader;
				this.setLogContext();
				logger.info(Log.INIT, "partition=" + multiReader.getPartition());
			}
			logger.info(Log.INIT, String.format("begin sync %s (%d rows)", 
					tableLoaderName, reader.getReaderMetrics().getExpected()));
			reader.call();
		}
		else /* Update or Insert */ {
			db.createMissingTable(table, sqlTableName);
			if (config.getTruncate()) db.truncateTable(sqlTableName);
			DatabaseTableWriter writer;
			if (JobAction.REFRESH.equals(action))
				writer = new DatabaseUpdateWriter(db, table, sqlTableName);
			else if (JobAction.LOAD.equals(action))
				writer = new DatabaseInsertWriter(db, table, sqlTableName);
			else
				throw new AssertionError();
			writer.setParentMetrics(metrics);
			writer.open();
			
			DateTime.Interval partitionInterval = config.getPartitionInterval();
			TableReaderFactory factory;
			DateTime since = config.getSince();			
			if (since != null) {
				factory = new KeySetTableReaderFactory(table, writer);
				factory.setUpdated(since);				
			}
			else {
				factory = new RestTableReaderFactory(table, writer);
			}
			factory.setReaderName(tableLoaderName);
			factory.setFilter(new EncodedQuery(table, config.getFilter()));
			factory.setCreated(createdRange);
			factory.setFields(fieldNames);
			factory.setPageSize(pageSize);
			TableReader reader;
			if (partitionInterval == null) {
				reader = factory.createReader();
				reader.setMaxRows(config.getMaxRows());
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
		}
		
		int processed = metrics.getProcessed();
		logger.info(Log.FINISH, String.format("end load %s (%d rows)", tableLoaderName, processed));
		Integer minRows = config.getMinRows();
		if (minRows != null && processed < minRows)
			throw new TooFewRowsException(table, minRows, processed);
		if (config.getSqlAfter() != null) {
			db.executeStatement(config.getSqlAfter());
		}
		return this.metrics;
	}

}
