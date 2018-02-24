package servicenow.datamart;

import servicenow.core.*;
import servicenow.json.KeySetTableReaderFactory;
import servicenow.rest.PartSumTableReader;
import servicenow.rest.RestTableReaderFactory;

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
		Log.setContext(table, tableLoaderName);
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
		default:
			throw new AssertionError();
		}
		db.createMissingTable(table, sqlTableName);
		writer.open();

		Log.setContext(table, tableLoaderName);		
		if (config.getTruncate()) db.truncateTable(sqlTableName);
		EncodedQuery filter;
		DateTime since = config.getSince();
		DateTime.Interval partitionInterval = config.getPartitionInterval();
		int pageSize = config.getPageSize() == null ? 0 : config.getPageSize().intValue();
		if (action == LoaderAction.PRUNE) {
			Table audit = session.table("sys_audit_delete");
			EncodedQuery auditQuery = new EncodedQuery("tablename", EncodedQuery.EQUALS, table.getName());
			reader = audit.getDefaultReader();
			reader.setBaseQuery(auditQuery);			
			reader.setCreatedRange(new DateTimeRange(since, null));
			if (pageSize > 0) reader.setPageSize(pageSize);
			reader.setWriter(writer);
			reader.initialize();
		}
		else {
			filter = config.getFilter();
			TableReaderFactory factory;
			if (since != null) {
				factory = new KeySetTableReaderFactory(table, writer);
				factory.setUpdated(since);				
			}
			else {
				factory = new RestTableReaderFactory(table, writer);
			}
			factory.setReaderName(tableLoaderName);
			factory.setBaseQuery(filter);
			factory.setCreated(config.getCreated());
			factory.setPageSize(pageSize);
			if (partitionInterval == null) {
				reader = factory.createReader();
				if (since != null) logger.info(Log.INIT, "getKeys " + reader.getQuery().toString());
				reader.initialize();
			}
			else {
				Integer threads = config.getThreads();
				reader = new PartSumTableReader(factory, partitionInterval, threads);
				factory.setParent(reader);
				reader.initialize();
				String partitionDescr = ((PartSumTableReader) reader).getPartition().toString();
				logger.info(Log.INIT, partitionDescr);
			}
		}
		assert reader != null;
		assert writer != null;
		assert reader.readerMetrics() != null;
		logger.info(Log.INIT, String.format("begin load %s (%d rows)", tableLoaderName, reader.readerMetrics().getExpected()));
		reader.call();
		writer.close();
		logger.info(Log.FINISH, String.format("end load %s (%d rows)", tableLoaderName, writer.getMetrics().getProcessed()));
		return writer.getMetrics();
	}

}
