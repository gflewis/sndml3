package sndml.datamart;

import sndml.servicenow.*;

public class SimpleTableLoader extends JobRunner implements Runnable {

	public SimpleTableLoader(ConnectionProfile profile, String tableName) {
		this(profile, profile.getSession().table(tableName), profile.getDatabase());		
	}
	
	public SimpleTableLoader(ConnectionProfile profile, Table table, Database database) {
		assert profile != null;
		assert table != null;
		ConfigFactory configFactory = new ConfigFactory(DateTime.now());
		this.config = configFactory.tableLoader(profile, table);
		this.session = table.getSession();
		this.db = database;
		this.table = table;
		this.sqlTableName = config.getTarget();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
		this.appRunLogger = null;
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
