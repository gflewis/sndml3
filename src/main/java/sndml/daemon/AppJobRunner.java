package sndml.daemon;

import java.io.IOException;
import java.sql.SQLException;

import sndml.datamart.*;
import sndml.servicenow.*;

public class AppJobRunner extends JobRunner implements Runnable {
	
	final AgentScanner scanner; // my parent
	final public RecordKey runKey;
	final public String number;
	AppStatusLogger statusLogger;
	
	/**
	 * Run a job with a new ServiceNow session and a new Database connection.
	 * Update ServiceNow with the status of the job as it runs.
	 */
	public AppJobRunner(AgentScanner scanner, ConnectionProfile profile, JobConfig config) {
		super(profile, config);
		this.scanner = scanner;
		this.profile = profile;
		this.runKey = config.getSysId();
		this.number = config.getNumber();
		assert runKey != null;
		assert runKey.isGUID();
		assert number != null;
		assert number.length() > 0;
	}

	void setSession(Session session) {
		this.session = session;
	}
	
	void setDatabase(Database database) {
		this.database = database;
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
			database.close();
		} catch (SQLException e) {
			throw new ResourceException(e);
		}
	}
	
	@Override
	public Metrics call() {
		String myName = this.getClass().getName() + ".call";
		assert profile != null;
		assert config.getNumber() != null;
		boolean onExceptionContinue = profile.getPropertyBoolean("daemon.continue", false);
		setThreadName();
		try {
			if (session == null) session = profile.getSession();
			statusLogger = new AppStatusLogger(profile, session);		
			if (database == null) database = profile.getDatabase();
			assert database != null;
			super.call();
			scanner.rescan();
		} catch (SQLException | IOException | InterruptedException e) {
			Log.setJobContext(this.getName());
			logger.error(Log.ERROR, myName + ": " + e.getClass().getName(), e);
			statusLogger.logError(runKey, e);
			if (!onExceptionContinue) AgentDaemon.abort();			
		} catch (Error e) {
			logger.error(Log.ERROR, myName + ": " + e.getClass().getName(), e);
			logger.error(Log.ERROR, "Critical error detected. Halting JVM.");
			Runtime.getRuntime().halt(-1);
		}
		return jobMetrics;
	}
	
	/**
	 * If this is not the main thread and it is not the scanner thread
	 * then change the thread name.
	 */
	private void setThreadName() {		
		// If this is not the main thread and it is not the scanner thread then change the thread name
		Thread myThread = Thread.currentThread();
		if (!myThread.equals(AgentDaemon.getThread()) && !myThread.getName().equals("scanner")) {
			myThread.setName(config.number);
		}
	}
		
}
