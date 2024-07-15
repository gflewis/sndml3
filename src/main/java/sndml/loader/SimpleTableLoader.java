package sndml.loader;

import sndml.servicenow.EncodedQuery;
import sndml.servicenow.RecordKey;
import sndml.servicenow.Table;
import sndml.util.DateTime;
import sndml.util.Log;
import sndml.util.ResourceException;

public class SimpleTableLoader extends JobRunner implements Runnable {
	
	public SimpleTableLoader(Resources resources, Table table, EncodedQuery filter) {
		super(resources, jobConfig(resources.getProfile(), table, filter));
	}
	
	public SimpleTableLoader(Resources resources, Table table, RecordKey docKey) {
		super(resources, jobConfig(resources.getProfile(), table, docKey));		
	}
		
	@Deprecated
	public SimpleTableLoader(ConnectionProfile profile, DatabaseWrapper database, Table table, EncodedQuery filter) {
		super(table.getSession(), database, jobConfig(profile, table, filter));
		this.table = table;
	}
	
	@Deprecated
	public SimpleTableLoader(ConnectionProfile profile, DatabaseWrapper database, Table table, RecordKey docKey) {
		super(table.getSession(), database, jobConfig(profile, table, docKey));
		this.table = table;
	}
	
	private static JobConfig jobConfig(ConnectionProfile profile, Table table, EncodedQuery query) {
		ConfigFactory configFactory = new ConfigFactory(DateTime.now());
		return configFactory.tableLoader(profile, table, query);		
	}
	
	private static JobConfig jobConfig(ConnectionProfile profile, Table table, RecordKey docKey) {
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
