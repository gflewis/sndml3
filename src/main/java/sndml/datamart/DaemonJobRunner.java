package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.*;

public class DaemonJobRunner extends JobRunner implements Runnable {
		
	public DaemonJobRunner(ConnectionProfile profile, JobConfig config) {
		this.session = profile.getSession();
		this.db = profile.getDatabase();
		this.config = config;
		this.runKey = config.getSysId();
		assert runKey != null;
		assert runKey.isGUID();
		this.appRunLogger = new AppRunLogger(profile, session, runKey);
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
		assert appRunLogger != null;
		assert config.getNumber() != null;
		WriterMetrics metrics = null;
		Thread.currentThread().setName(config.number);
		try {
			appRunLogger.setStatus("running");
			metrics = super.call();
			appRunLogger.setStatus("complete");
			Daemon.rescan();
		} catch (SQLException | IOException | InterruptedException e) {
			Log.setJobContext(this.getName());
			logger.error(Log.RESPONSE, e.toString(), e);
			appRunLogger.logError(e);
		}
		return metrics;
	}
		
}
