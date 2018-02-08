package servicenow.datamart;

import servicenow.core.*;
import servicenow.json.JsonKeyedReader;
import servicenow.rest.MultiDatePartReader;
import servicenow.rest.RestTableReader;

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
	
	TableReader reader;
	TableWriter writer;
	
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
	
	TableLoader(TableConfig config) {		
		this.session = ResourceManager.getSession();
		this.db = ResourceManager.getDatabaseWriter();
		this.table = session.table(config.getSource());
		this.sqlTableName = config.getTargetName();
		this.tableLoaderName = config.getName();
		this.config = config;
	}
	
	public String getName() {
		return this.table.getName();
	}
	
	public WriterMetrics getMetrics() {
		return writer.getMetrics();
	}
		
	public WriterMetrics call() throws SQLException, IOException, InterruptedException {
		assert sqlTableName != null;
		assert sqlTableName.length() > 0;
		Log.clearContext();
		Log.setTableContext(table.getName());
		Log.setWriterContext(tableLoaderName);
		LoaderAction action = config.getAction();
		assert action != null;
		logger.debug(Log.INIT, 
			String.format("call table=%s action=%s", table.getName(), action.toString()));;
		switch (action) {
		case UPDATE: 
			writer = new TableUpdateWriter(tableLoaderName, db, table, sqlTableName);
			break;
		case INSERT:
			writer = new TableInsertWriter(tableLoaderName, db, table, sqlTableName);
			break;
		case PRUNE:
			writer = new TableDeleteWriter(tableLoaderName, db, table, sqlTableName);
			break;
		}
		db.createMissingTable(table, sqlTableName);
		writer.open();
		if (config.getTruncate()) db.truncateTable(sqlTableName);
		EncodedQuery filter;
		DateTime since = config.getSince();
		DateTime.Interval partitionInterval = config.getPartitionInterval();
		Integer pageSize = config.getPageSize();
		if (action == LoaderAction.PRUNE) {
			Table audit = session.table("sys_audit_delete");
			EncodedQuery auditQuery = new EncodedQuery("tablename", EncodedQuery.EQUALS, table.getName());
			reader = audit.getDefaultReader();
			reader.setBaseQuery(auditQuery);			
			reader.setCreatedRange(new DateTimeRange(since, null));
			if (pageSize != null) reader.setPageSize(pageSize);
			reader.setWriter(writer);
			reader.initialize();
		}
		else {
			DateTimeRange created = config.getCreated();
			DateTimeRange updated = new DateTimeRange(since, null);
			filter = config.getFilter();
			if (partitionInterval == null) {
				if (since == null) {
					reader = new RestTableReader(table).enableStats(true);
				}
				else
					reader = new JsonKeyedReader(table);
				reader.setBaseQuery(filter);
				reader.setCreatedRange(created);
				reader.setUpdatedRange(updated);
				if (pageSize != null) reader.setPageSize(pageSize);
				reader.setWriter(writer);
				if (since != null) 
					logger.info(Log.INIT, "getKeys " + reader.getQuery().toString());
				reader.initialize();
			}
			else {
				Integer threads = config.getThreads();
				MultiDatePartReader mReader = 
						new MultiDatePartReader(table, partitionInterval, filter, created, updated, threads, writer);
				if (pageSize != null) reader.setPageSize(pageSize);
				mReader.initialize();				
				logger.info(Log.INIT, mReader.getPartition().toString());
				reader = mReader;
			}
		}
		logger.info(Log.INIT, String.format("begin load %s (%d rows)", tableLoaderName, reader.readerMetrics().getExpected()));
		reader.call();
		writer.close();
		logger.info(Log.TERM, String.format("end load %s (%d rows)", tableLoaderName, writer.getMetrics().getProcessed()));
		return writer.getMetrics();
	}

}
