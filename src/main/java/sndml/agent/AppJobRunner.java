package sndml.agent;

import java.io.IOException;
import java.sql.SQLException;

import sndml.loader.*;
import sndml.servicenow.*;
import sndml.util.Log;
import sndml.util.Metrics;
import sndml.util.ProgressLogger;
import sndml.util.ResourceException;

public class AppJobRunner extends JobRunner {

	protected final AppJobConfig config;
	
	final ConnectionProfile profile;
	final AppSession appSession;
	final RecordKey runKey;
	final String number;
	final AppStatusLogger statusLogger;
	Thread runnerThread;

	public AppJobRunner(Resources resources, AppJobConfig config) {
		super(resources, config);
		this.profile = resources.getProfile();
		this.appSession = resources.getAppSession();
		this.config = config;
		this.statusLogger = new AppStatusLogger(appSession);				
		this.runKey = config.getRunKey();
		this.number = config.getNumber();
		assert runKey != null : "runKey is null";
		assert runKey.isGUID() : "runKey is not a GUID";
		assert number != null : "number is null";
		assert number.length() > 0 : "number is empty";
	}
	
	String getNumber() {
		return number;
	}
	
	Thread getThread() {
		assert runnerThread != null : "runnerThread not initialized";
		return runnerThread;
	}
	
	/**
	 * Cancel this AppJobRunner
	 */
	void interrupt() {
		assert runnerThread != null : "runnerThread not initialized";
		assert runnerThread != Thread.currentThread() : "cannot interrupt self";
		runnerThread.interrupt();
	}
	
	AppStatusLogger getStatusLogger() {
		return this.statusLogger;
	}
		
	@Override
	protected ProgressLogger createJobProgressLogger(TableReader reader) {
		assert action != null : "action is null";
		assert jobMetrics != null : "jobMetrics is null";
		Log4jProgressLogger textLogger;
		AppProgressLogger appLogger;
		if (reader != null) {
			textLogger = new Log4jProgressLogger(reader.getClass(), action, jobMetrics);					
		}
		else {
			textLogger =  new Log4jProgressLogger(this.getClass(), action, jobMetrics);
		}
		appLogger =	new AppProgressLogger(appSession, jobMetrics, number, runKey);
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
		runnerThread = Thread.currentThread();
		String name = config.getNumber();
		if (!runnerThread.equals(AgentMain.getThread()) && !runnerThread.getName().equals("scanner")) {
			runnerThread.setName(name);
		}
	}

	@Override
	public void close() throws ResourceException {
		// Close the database connection
		if (resources.isWorkerCopy()) resources.close();
	}		
	
	@Override
	public Metrics call() 
			throws SQLException, IOException, JobCancelledException {
		String myName = this.getClass().getName() + ".call";
		setThreadName();
		Log.setJobContext(config);
		logger.info(Log.INIT, "call " + number);
		Metrics metrics = null;
		// TODO Why are we unable to detect the interrupt?
		try {
			metrics = super.call();
		}
		catch (InterruptedException e) {
			logger.error(Log.FINISH, String.format("%s Interrupt Detected", number));
		} 
		catch (JobCancelledException e) {
			logger.error(Log.FINISH, String.format("%s Job Cancel Detected", number));
			throw e;
		}
		catch (Throwable e) {
			logger.error(Log.ERROR, myName + ": " + e.getClass().getName(), e);
			logger.error(Log.ERROR, "Critical error detected. Halting JVM.");
			Log.shutdown(); // Flush the logs
			Runtime.getRuntime().halt(-1);
		}
		// TODO Is this debugging code or what?
		if (Thread.interrupted()) {
			System.out.println(String.format("%s Was Interrupted", number));
			System.out.flush();			
		}
		if (resources.isWorkerCopy()) resources.close();
		return metrics;
	}
	
		
}
