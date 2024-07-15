package sndml.agent;

import java.sql.SQLException;

import sndml.loader.*;
import sndml.servicenow.*;
import sndml.util.Log;
import sndml.util.ResourceException;

public class AppJobRunner extends JobRunner implements Runnable {

	protected final AppJobConfig config;
	
	final ConnectionProfile profile;
	final AppSession appSession;
	final public RecordKey runKey;
	final public String number;
	final AppStatusLogger statusLogger;

	public AppJobRunner(Resources resources, AppJobConfig config) {
		super(resources, config);
		this.profile = resources.getProfile();
		this.appSession = resources.getAppSession();
		this.config = config;
		this.statusLogger = new AppStatusLogger(appSession);				
		this.runKey = config.getSysId();
		this.number = config.getNumber();
		assert runKey != null;
		assert runKey.isGUID();
		assert number != null;
		assert number.length() > 0;		
	}
		
	@Deprecated
	public AppJobRunner(ConnectionProfile profile, AppSession appSession, ReaderSession readerSession, 
			DatabaseWrapper databaseConnection, AppJobConfig config) {
		super(readerSession, databaseConnection, config);
		this.profile = profile;
		this.appSession = appSession; 
		this.config = config;
		this.statusLogger = new AppStatusLogger(appSession);				
		this.runKey = config.getSysId();
		this.number = config.getNumber();
		assert runKey != null;
		assert runKey.isGUID();
		assert number != null;
		assert number.length() > 0;		
	}
	
	@Deprecated
	AppJobRunner(ConnectionProfile profile, AppJobConfig config) {
		this(profile, profile.newAppSession(), profile.newReaderSession(), 
				profile.newDatabaseWrapper(), config);
	}
	
	AppStatusLogger getStatusLogger() {
		return this.statusLogger;
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
		appLogger =	new AppProgressLogger(profile, appSession, jobMetrics, number, runKey);
		assert appLogger.getMetrics() == jobMetrics;
		ProgressLogger compositeLogger = new CompositeProgressLogger(textLogger, appLogger);
		return compositeLogger;
	}

	/**
	 * If this is not the main thread and it is not the scanner thread
	 * then change the thread name.
	 */
	protected void setThreadName() {		
		// If this is not the main thread and it is not the scanner thread then change the thread name
		Thread myThread = Thread.currentThread();
		if (!myThread.equals(AgentDaemon.getThread()) && !myThread.getName().equals("scanner")) {
			myThread.setName(config.number);
		}
	}
	
	@Override
	public void run() {
		setThreadName();
		try {
			this.call();
		} catch (JobCancelledException e) {
			logger.warn(Log.ERROR, "Job Cancellation Detected");
		} catch (ResourceException e) {
			logger.error(Log.ERROR, e.getMessage(), e);
			e.printStackTrace(System.err);
		} catch (Exception e) {
			logger.error(Log.ERROR, e.getMessage(), e);			
			e.printStackTrace(System.err);
		}
	}
	
	@Override
	public void close() throws ResourceException {
		// Close the database connection
		try {
			database.close();
		} catch (SQLException e) {
			throw new ResourceException(e);
		}
	}
	
	/*
	@Override
	public Metrics call() throws JobCancelledException {
		String myName = this.getClass().getName() + ".call";
		assert profile != null;
		assert config.getNumber() != null;
		boolean onExceptionContinue = profile.agent.getBoolean("continue", false);
		setThreadName();
		try {
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
	*/
	
	
	
		
}
