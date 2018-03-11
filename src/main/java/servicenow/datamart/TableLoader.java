package servicenow.datamart;

import servicenow.api.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableLoader implements Callable<WriterMetrics> {

	private final Session session;
	private final Database db;
	private final Table table;
	private final String sqlTableName;
	private final String tableLoaderName;
	private final TableConfig config;
	private WriterMetrics metrics;
	
	Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public static void main(String[] args) throws Exception {
		Log.setGlobalContext();
		Options options = new Options();
		options.addOption(Option.builder("p").longOpt("prop").required(true).hasArg(true).
				desc("Property file (required)").build());
		options.addOption(Option.builder("t").longOpt("table").required(true).hasArg(true).
				desc("Table name").build());		
		Globals.initialize(options, args);
		ResourceManager.initialize(Globals.getProperties());
		String tablename = Globals.getOptionValue("t");
		TableLoader loader = new TableLoader(ResourceManager.getSession().table(tablename));
		loader.call();
	}
	
	TableLoader(Table table) throws ConfigParseException {
		this(new TableConfig(null, table.getName()));
	}
	
	TableLoader(TableConfig config) throws ConfigParseException {
		config.validate();
		this.session = ResourceManager.getSession();
		this.db = ResourceManager.getDatabase();
		this.table = session.table(config.getSource());
		this.sqlTableName = config.getTargetName();
		this.tableLoaderName = config.getName();
		this.config = config;
	}
	
	String getName() {
		return this.table.getName();
	}
	
	TableConfig getConfig() {
		return this.config;
	}
	
	WriterMetrics getMetrics() {
		assert metrics != null;
		return metrics;
	}
		
	public WriterMetrics call() throws SQLException, IOException, InterruptedException {
		assert sqlTableName != null;
		assert sqlTableName.length() > 0;
		Log.clearContext();
		Log.setContext(table, tableLoaderName);
		LoaderAction action = config.getAction();
		assert action != null;

		logger.debug(Log.INIT, 
			String.format("call table=%s action=%s", table.getName(), action.toString()));
		if (config.getSqlBefore() != null) {
			db.executeStatement(config.getSqlBefore());
		}
		db.createMissingTable(table, sqlTableName);

		Log.setContext(table, tableLoaderName);	
		if (config.getTruncate()) db.truncateTable(sqlTableName);
		DateTime since = config.getSince();
		
		if (LoaderAction.PRUNE.equals(action)) {
			DatabaseDeleteWriter deleteWriter = new DatabaseDeleteWriter(db, table, sqlTableName);
			deleteWriter.open();
			Table audit = session.table("sys_audit_delete");
			EncodedQuery auditQuery = new EncodedQuery("tablename", EncodedQuery.EQUALS, table.getName());
			RestTableReader auditReader = new RestTableReader(audit);
			auditReader.enableStats(true);
			auditReader.setBaseQuery(auditQuery);			
			auditReader.setCreatedRange(new DateTimeRange(since, null));
			auditReader.setMaxRows(config.getMaxRows());
			auditReader.setWriter(deleteWriter);
			auditReader.initialize();
			logger.info(Log.INIT, String.format("begin delete %s (%d rows)", 
					tableLoaderName, auditReader.getReaderMetrics().getExpected()));
			auditReader.call();
			deleteWriter.close();
			metrics = deleteWriter.getMetrics();;		
		}
		else if (LoaderAction.SYNC.equals(action)) {
			DateTimeRange createdRange = config.getCreated();
			TableSyncReader syncReader = new TableSyncReader(table, db, sqlTableName);
			syncReader.initialize(createdRange);
			logger.info(Log.INIT, String.format("begin sync %s (%d rows)", 
					tableLoaderName, syncReader.getReaderMetrics().getExpected()));
			syncReader.call();
			metrics = syncReader.getWriterMetrics();						
		}
		else {
			DatabaseTableWriter writer;
			if (LoaderAction.UPDATE.equals(action))
				writer = new DatabaseUpdateWriter(db, table, sqlTableName);
			else if (LoaderAction.INSERT.equals(action))
				writer = new DatabaseInsertWriter(db, table, sqlTableName);
			else
				throw new AssertionError();
			writer.open();
			
			DateTime.Interval partitionInterval = config.getPartitionInterval();
			int pageSize = config.getPageSize() == null ? 0 : config.getPageSize().intValue();								
			TableReaderFactory factory;
			if (since != null) {
				factory = new KeySetTableReaderFactory(table, writer);
				factory.setUpdated(since);				
			}
			else {
				factory = new RestTableReaderFactory(table, writer);
			}
			factory.setReaderName(tableLoaderName);
			factory.setBaseQuery(config.getFilter());
			factory.setCreated(config.getCreated());
			factory.setOrderBy(config.getOrderBy());
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
			metrics = writer.getMetrics();
			
		}
		
		int processed = metrics.getProcessed();
		logger.info(Log.FINISH, String.format("end load %s (%d rows)", tableLoaderName, processed));
		Integer minRows = config.getMinRows();
		if (minRows != null && processed < minRows)
			throw new ServiceNowException(
				String.format("%d rows were processed (MinRows=%d)",  processed, minRows));		
		if (config.getSqlAfter() != null) {
			db.executeStatement(config.getSqlAfter());
		}
		return metrics;
	}

}
