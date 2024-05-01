package sndml.agent;

import java.io.IOException;
import java.sql.SQLException;

import sndml.loader.*;
import sndml.servicenow.*;
import sndml.util.Log;

public class AgentJobRunner extends JobRunner implements Runnable {
	
	final ConnectionProfile profile;
	final AgentScanner scanner; // my parent
	final public RecordKey runKey;
	final public String number;
	AppStatusLogger statusLogger;

	// TODO Remove
	/**
	 * This class uses a composite progress logger which writes to Log4J2
	 * and also updates ServiceNow with the status of the job as it runs.
	 *
	 * Creation of Session and DatabaseConnection is deferred until the "call" method. 
	 * The session and database variables are initialized in the "call" method if 
	 * they are null.
	 * 
	 */
	/*
	AgentJobRunner(AgentScanner scanner, ConnectionProfile profile, JobConfig config) {
		super(profile.newReaderSession(), profile.newDatabaseConnection(), config);
		this.scanner = scanner;
		this.runKey = config.getSysId();
		this.number = config.getNumber();
		assert runKey != null;
		assert runKey.isGUID();
		assert number != null;
		assert number.length() > 0;
	}
	*/
	
	AgentJobRunner(AgentScanner scanner, ConnectionProfile profile, JobConfig config) {
		super(profile.newReaderSession(), profile.newDatabaseConnection(), config);
		this.profile = profile;
		this.scanner = scanner;
		this.runKey = config.getSysId();
		this.number = config.getNumber();
		assert runKey != null;
		assert runKey.isGUID();
		assert number != null;
		assert number.length() > 0;
	}
	
	/*
	protected void setSession(Session session) {
		this.readerSession = session;
	}
	
	protected void setDatabase(DatabaseConnection database) {
		this.database = database;
	}
	*/
	
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
		appLogger =	new AppProgressLogger(profile, readerSession, jobMetrics, number, runKey);
		assert appLogger.getMetrics() == jobMetrics;
		ProgressLogger compositeLogger = new CompositeProgressLogger(textLogger, appLogger);
		return compositeLogger;
	}
		
	@Override
	public void run() {
		try {
			this.call();
		} catch (JobCancelledException e) {
			logger.warn(Log.ERROR, "Job Cancellation Detected");
			throw new ResourceException(e);
		}
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
	public Metrics call() throws JobCancelledException {
		String myName = this.getClass().getName() + ".call";
		assert profile != null;
		assert config.getNumber() != null;
		boolean onExceptionContinue = profile.agent.getBoolean("continue", false);
		setThreadName();
		try {
			statusLogger = new AppStatusLogger(profile, readerSession);		
			super.call();
			if (scanner != null) scanner.rescan();
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