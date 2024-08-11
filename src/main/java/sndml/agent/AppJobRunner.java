package sndml.agent;

import java.io.IOException;
import java.sql.SQLException;

import sndml.loader.*;
import sndml.servicenow.*;
import sndml.util.Log;
import sndml.util.Metrics;
import sndml.util.ResourceException;

public class AppJobRunner extends JobRunner implements Runnable {

	protected final AppJobConfig config;
	
	final ConnectionProfile profile;
	final AppSession appSession;
	final RecordKey runKey;
	final String number;
	final AppStatusLogger statusLogger;
	Thread myThread;

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
	
	String getNumber() {
		return number;
	}
	
	Thread getThread() {
		return myThread;
	}
	
	/**
	 * Cancel this AppJobRunner
	 */
	void interrupt() {
		assert myThread != null;
		assert myThread != Thread.currentThread();
		myThread.interrupt();
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
	
	protected void setThreadName(String name) {		
		// If this is not the main thread and it is not the scanner thread then change the thread name
		if (myThread != null && !myThread.equals(AgentMain.getThread()) && 
				!myThread.getName().equals("scanner")) {
			myThread.setName(name);
		}
	}

	protected void setThread() {
		myThread = Thread.currentThread();
		setThreadName(config.getName());		
	}	
	
	@Override
	public void run() {
		setThread();
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
	public Metrics call() 
			throws SQLException, IOException, JobCancelledException {
		Metrics metrics = null;
		setThread();
		// TODO Why are we unable to detect the interrupt?
		try {
			metrics = super.call();
		}
		catch (InterruptedException e) {
			logger.error(Log.FINISH, String.format("%s Interrupt Detected", number));
			System.out.println(String.format("%s Interrupt Detected", number));
			System.out.flush();
		}
		if (Thread.interrupted()) {
			System.out.println(String.format("%s Was Interrupted", number));
			System.out.flush();			
		}
		return metrics;
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
		
}
