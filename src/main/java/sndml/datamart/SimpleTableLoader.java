package sndml.datamart;

import sndml.servicenow.*;

public class SimpleTableLoader extends JobRunner implements Runnable {

	public SimpleTableLoader(ConnectionProfile profile, String tableName, Database database) {
		this(profile, profile.getSession().table(tableName), database);		
	}
	
	public SimpleTableLoader(ConnectionProfile profile, Table table, Database database) {
		super(table.getSession(), database, jobConfig(profile, table));
		this.table = table;
	}
	
	private static JobConfig jobConfig(ConnectionProfile profile, Table table) {
		ConfigFactory configFactory = new ConfigFactory(DateTime.now());
		return configFactory.tableLoader(profile, table);		
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
