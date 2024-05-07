package sndml.loader;

import sndml.servicenow.SchemaFactory;
import sndml.servicenow.Table;
import sndml.servicenow.TableSchemaReader;
import sndml.util.DateTime;
import sndml.util.Log;
import sndml.util.ResourceException;

public class SimpleTableLoader extends JobRunner implements Runnable {

	public SimpleTableLoader(ConnectionProfile profile, DatabaseConnection database, String tableName) {
		this(profile, database, profile.newReaderSession().table(tableName), null);		
	}
	
	public SimpleTableLoader(ConnectionProfile profile, DatabaseConnection database, String tableName, String filter) {
		this(profile, database, profile.newReaderSession().table(tableName), filter);		
	}
	
	public SimpleTableLoader(ConnectionProfile profile, DatabaseConnection database, Table table, String filter) {
		super(table.getSession(), database, jobConfig(profile, table, filter));
		this.table = table;
	}
	
	private static JobConfig jobConfig(ConnectionProfile profile, Table table, String filter) {
		SchemaFactory.setSchemaReader(new TableSchemaReader(table.getSession()));
		ConfigFactory configFactory = new ConfigFactory(DateTime.now());
		return configFactory.tableLoader(profile, table, filter);		
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
