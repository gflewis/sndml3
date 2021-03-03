package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.*;

public class DaemonJobRunner extends JobRunner implements Runnable {
	
	final Key runKey;
	final String number;
	AppRunLogger appRunLogger;
		
	public DaemonJobRunner(ConnectionProfile profile, JobConfig config) {
		super(profile.getSession(), profile.getDatabase(), config);
		this.runKey = config.getSysId();
		this.number = config.getNumber();
		assert runKey != null;
		assert runKey.isGUID();
		assert number != null;
		assert number.length() > 0;
		this.appRunLogger = new AppRunLogger(profile, session, number, runKey);
		this.table = session.table(config.getSource());
	}

	@Override
	protected ProgressLogger newProgressLogger(TableReader reader) {
		assert appRunLogger != null;
		assert reader != null;
		ProgressLogger progressLogger = new CompositeProgressLogger(reader, appRunLogger);
		reader.setProgressLogger(progressLogger);
		return progressLogger;
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
	public DaemonJobRunner call() {
		assert session != null;
		assert db != null;
		assert appRunLogger != null;
		assert config.getNumber() != null;
		Thread.currentThread().setName(config.number);
		try {
			appRunLogger.setStatus("running");
			super.call();
			appRunLogger.setStatus("complete");
			Daemon.rescan();
		} catch (SQLException | IOException | InterruptedException e) {
			Log.setJobContext(this.getName());
			logger.error(Log.RESPONSE, e.toString(), e);
			appRunLogger.logError(e);
		}
		return this;
	}
		
}
