package sndml.datamart;

import sndml.servicenow.WriterMetrics;

public class JobConfigRunner extends JobRunner {

	public JobConfigRunner(ConnectionProfile profile, JobConfig config) {
		super(profile.getSession(), profile.getDatabase(), config);
		this.table = session.table(config.getSource());
		this.sqlTableName = config.getTarget();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
		this.appRunLogger = null;				
	}
		
}
