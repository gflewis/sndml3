package sndml.loader;

import sndml.servicenow.EncodedQuery;
import sndml.servicenow.RecordKey;
import sndml.servicenow.SchemaFactory;
import sndml.servicenow.Table;
import sndml.servicenow.TableSchemaReader;
import sndml.util.DateTime;
import sndml.util.Log;
import sndml.util.ResourceException;

public class SimpleTableLoader extends JobRunner implements Runnable {
	
	public SimpleTableLoader(ConnectionProfile profile, DatabaseConnection database, Table table, EncodedQuery filter) {
		super(table.getSession(), database, jobConfig(profile, table, filter));
		this.table = table;
	}
	
	public SimpleTableLoader(ConnectionProfile profile, DatabaseConnection database, Table table, RecordKey docKey) {
		super(table.getSession(), database, jobConfig(profile, table, docKey));
		this.table = table;
	}
	
	private static JobConfig jobConfig(ConnectionProfile profile, Table table, EncodedQuery query) {
		SchemaFactory.setSchemaReader(new TableSchemaReader(table.getSession()));
		ConfigFactory configFactory = new ConfigFactory(DateTime.now());
		return configFactory.tableLoader(profile, table, query);		
	}
	
	private static JobConfig jobConfig(ConnectionProfile profile, Table table, RecordKey docKey) {
		SchemaFactory.setSchemaReader(new TableSchemaReader(table.getSession()));
		ConfigFactory configFactory = new ConfigFactory(DateTime.now());
		return configFactory.singleRecordSync(profile, table, docKey);		
	}

	@Override
	public void run() {
		try {
			super.call();
		} catch (Exception e) {
			logger.error(Log.INIT, e.getMessage(), e);
			throw new ResourceException(e);
		}
	}

}
