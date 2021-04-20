package sndml.daemon;

import java.io.IOException;
import java.sql.SQLException;

import sndml.datamart.CompositeProgressLogger;
import sndml.datamart.ConnectionProfile;
import sndml.datamart.JobConfig;
import sndml.datamart.JobRunner;
import sndml.datamart.Log4jProgressLogger;
import sndml.datamart.ResourceException;
import sndml.servicenow.*;

public class AppJobRunner extends JobRunner implements Runnable {
	
	final AgentScanner scanner; // my parent
	final ConnectionProfile profile;
	final public RecordKey runKey;
	final public String number;
	final AppStatusLogger statusLogger;
	
	/**
	 * Run a job with a new ServiceNow session and a new Database connection.
	 * Update ServiceNow with the status of the job as it runs.
	 */
	public AppJobRunner(AgentScanner scanner, ConnectionProfile profile, JobConfig config) {
		super(profile.getSession(), profile.getDatabase(), config);
		this.scanner = scanner;
		this.profile = profile;
		this.runKey = config.getSysId();
		this.number = config.getNumber();
		assert runKey != null;
		assert runKey.isGUID();
		assert number != null;
		assert number.length() > 0;
		this.table = session.table(config.getSource());
		this.statusLogger = new AppStatusLogger(profile, session);		
	}

	@Override
	protected ProgressLogger createJobProgressLogger(TableReader reader) {
		assert action != null;
		assert jobMetrics != null;
		Log4jProgressLogger textLogger;
		AppProgressLogger appLogger;
		if (reader != null) {
			textLogger = new Log4jProgressLogger(reader.getClass(), action, jobMetrics);					
		}
		else {
			textLogger =  new Log4jProgressLogger(this.getClass(), action, jobMetrics);
		}
		appLogger =	new AppProgressLogger(profile, session, jobMetrics, number, runKey);
		assert appLogger.getMetrics() == jobMetrics;
		ProgressLogger compositeLogger = new CompositeProgressLogger(textLogger, appLogger);
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
			Metrics metrics = super.call();
			scanner.rescan();
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
