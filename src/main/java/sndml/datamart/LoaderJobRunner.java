package sndml.datamart;

@Deprecated
public class LoaderJobRunner extends JobRunner {


	public LoaderJobRunner(Loader parent, JobConfig config) throws ConfigParseException {
		super(parent.getSession(), parent.getDatabase(), config);
		this.table = session.table(config.getSource());
//		this.sqlTableName = config.getTarget();
//		this.tableLoaderName = config.getName();
//		this.metrics = new WriterMetrics();
//		this.metrics.setParent(parent.getMetrics());
//		this.appRunLogger = null;		
	}

}
