package sndml.datamart;

import sndml.servicenow.WriterMetrics;

public class LoaderJobRunner extends JobRunner {


	public LoaderJobRunner(Loader parent, JobConfig config) throws ConfigParseException {
		this.session = parent.getSession();
		this.db = parent.getDatabase();
		this.table = session.table(config.getSource());
		this.sqlTableName = config.getTarget();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
		this.metrics.setParent(parent.getMetrics());
		this.config = config;
		this.appRunLogger = null;		
	}

}
