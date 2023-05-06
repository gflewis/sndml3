package sndml.datamart;

import sndml.servicenow.Table;
import sndml.util.DateTime;
import sndml.util.Log;

public class SimpleTableLoader extends JobRunner implements Runnable {

	public SimpleTableLoader(ConnectionProfile profile, Database database, String tableName) {
		this(profile, database, profile.getSession().table(tableName), null);		
	}
	
	public SimpleTableLoader(ConnectionProfile profile, Database database, String tableName, String filter) {
		this(profile, database, profile.getSession().table(tableName), filter);		
	}
	
	public SimpleTableLoader(ConnectionProfile profile, Database database, Table table, String filter) {
		super(table.getSession(), database, jobConfig(profile, table, filter));
		this.table = table;
	}
	
	private static JobConfig jobConfig(ConnectionProfile profile, Table table, String filter) {
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
