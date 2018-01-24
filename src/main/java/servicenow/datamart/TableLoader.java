package servicenow.datamart;

import servicenow.core.*;
import servicenow.rest.MultiDatePartReader;
import servicenow.rest.RestTableAPI;

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
		this.table = session.table(config.getName());
		this.config = config;
	}
	
	public String getName() {
		return this.table.getName();
	}
	
	public WriterMetrics getMetrics() {
		return writer.getMetrics();
	}
		
	public WriterMetrics call() throws SQLException, IOException, InterruptedException {
		Log.setTableContext(table);
		RestTableAPI impl = table.rest();
		String targetName = config.getTargetName();
		assert targetName != null;
		assert targetName.length() > 0;
		LoaderAction action = config.getAction();
		assert action != null;
		logger.debug(Log.INIT, 
			String.format("call table=%s action=%s", table.getName(), action.toString()));;
		switch (action) {
		case UPDATE: 
			writer = new TableUpdateWriter(db, table, targetName);
			break;
		case INSERT:
			writer = new TableInsertWriter(db, table, targetName);
			break;
		case PRUNE:
			writer = new TableDeleteWriter(db, table, targetName);
			break;
		}
		db.createMissingTable(table, targetName);
		writer.open();
		if (config.getTruncate()) db.truncateTable(targetName);
		EncodedQuery filter;
		DateTime.Interval partitionInterval = config.getPartitionInterval();
		if (action == LoaderAction.PRUNE) {
			Table audit = session.table("sys_audit_delete");
			reader = audit.getDefaultReader();
			reader.setBaseQuery(new EncodedQuery("tablename", EncodedQuery.EQUALS, table.getName()));
			reader.setCreatedRange(config.getSince());			
		}
		else {
			DateTimeRange created = config.getCreated();
			DateTimeRange updated = DateTimeRange.all().
					intersect(config.getUpdated()).
					intersect(config.getSince());
			filter = config.getFilter();
			if (partitionInterval == null) {
				reader = table.getDefaultReader();
				reader.setBaseQuery(filter);
				reader.setCreatedRange(created);
				reader.setUpdatedRange(updated);
				reader.setWriter(writer);
			}
			else {
				Integer threads = config.getThreads();
				reader = new MultiDatePartReader(impl, partitionInterval, filter, created, updated, threads, writer);			
			}
		}
		if (config.getPageSize() != null) reader.setPageSize(config.getPageSize());
		reader.initialize();
		logger.info(Log.INIT, String.format("begin load %s (%d rows)", targetName, reader.getMetrics().getExpected()));
		reader.call();
		writer.close();
		logger.info(Log.TERM, String.format("end load %s (%d rows)", targetName, writer.getMetrics().getProcessed()));
		return writer.getMetrics();
	}

}
