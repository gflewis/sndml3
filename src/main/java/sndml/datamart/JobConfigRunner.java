package sndml.datamart;

import sndml.servicenow.WriterMetrics;

public class JobConfigRunner extends JobRunner {

	public JobConfigRunner(ConnectionProfile profile, JobConfig config) {
		this.session = profile.getSession();
		this.db = profile.getDatabase();
		this.table = session.table(config.getSource());
		this.sqlTableName = config.getTarget();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
		this.config = config;
		this.appRunLogger = null;				
	}
		
}
