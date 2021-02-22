package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.*;

public class DaemonJobRunner extends JobRunner implements Runnable {
	
	private final Key runKey;
	private final AppRunLogger runLogger;
	
	public DaemonJobRunner(ConnectionProfile profile, JobConfig config) {
		this.session = profile.getSession();
		this.db = profile.getDatabase();
		this.config = config;
		this.runKey = config.getSysId();
		assert runKey != null;
		assert runKey.isGUID();
		this.runLogger = new AppRunLogger(profile, session, runKey);
		this.table = session.table(config.getSource());
		this.sqlTableName = config.getTarget();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
	}

	@Override
	public void run() {
		this.call();
	}
		
	@Override
	public WriterMetrics call() {
		assert session != null;
		assert db != null;
		assert runLogger != null;
		WriterMetrics metrics = null;
		try {
			runLogger.setStatus("running");
			metrics = super.call();
			runLogger.setStatus("complete");
		} catch (SQLException | IOException | InterruptedException e) {
			Log.setJobContext(this.getName());
			logger.error(Log.RESPONSE, e.toString(), e);
			runLogger.logError(e);
		}
		return metrics;
	}
		
}
