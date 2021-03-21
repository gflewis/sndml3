package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.*;

public class DaemonJobRunner extends JobRunner implements Runnable {
	
	final ConnectionProfile profile;
	final Key runKey;
	final String number;
	final DaemonStatusLogger statusLogger;
	DaemonProgressLogger appLogger;
		
	public DaemonJobRunner(ConnectionProfile profile, JobConfig config) {
		super(profile.getSession(), profile.getDatabase(), config);
		this.profile = profile;
		this.runKey = config.getSysId();
		this.number = config.getNumber();
		assert runKey != null;
		assert runKey.isGUID();
		assert number != null;
		assert number.length() > 0;
		this.table = session.table(config.getSource());
		this.statusLogger = new DaemonStatusLogger(profile, session);
		this.appLogger = new DaemonProgressLogger(profile, session, jobMetrics, number, runKey);
		
	}

	@Override
	protected ProgressLogger newProgressLogger(TableReader reader) {
		Log4jProgressLogger textLogger = 
			new Log4jProgressLogger(reader.getClass(), action, jobMetrics);		
		ProgressLogger compositeLogger = new CompositeProgressLogger(textLogger, appLogger);
		reader.setMetrics(jobMetrics);;
		reader.setProgressLogger(compositeLogger);
		return compositeLogger;
	}
		
	@Override
	public void run() {
		this.call();
	}

	@Override
	protected void close() throws ResourceException {
		// Close the database connection
		try {
			db.close();
		} catch (SQLException e) {
			throw new ResourceException(e);
		}
	}
	
	@Override
	public Metrics call() {
		try {
			assert session != null;
			assert db != null;
			assert config.getNumber() != null;
			Thread.currentThread().setName(config.number);			
//			statusLogger.setStatus(runKey, "running");
			Metrics metrics = super.call();
//			statusLogger.setStatus(runKey, "complete");
			Daemon.rescan();
			return metrics;
		} catch (SQLException | IOException | InterruptedException e) {
			Log.setJobContext(this.getName());
			logger.error(Log.FINISH, e.toString(), e);
			statusLogger.logError(runKey, e);
			return null;
		} catch (Error e) {
			logger.error(Log.FINISH, e.toString(), e);			
			logger.error(Log.FINISH, "Critical error detected. Halting JVM.");
			Runtime.getRuntime().halt(-1);
			return null;
		}
	}
		
}
