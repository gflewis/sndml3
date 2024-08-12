package sndml.agent;

import java.io.IOException;
import java.sql.SQLException;

import sndml.loader.Resources;
import sndml.util.Log;
import sndml.util.Metrics;

/**
 * An AppJobRunner which performs a re-scan after a job is complete
 */
public class ScannerJobRunner extends AppJobRunner {
	
	final AgentScanner scanner; // my parent
	final boolean onExceptionContinue;
	

	public ScannerJobRunner(AgentScanner scanner, Resources resources, AppJobConfig config) {
		super(resources, config);
		this.scanner = scanner;
		onExceptionContinue = Boolean.parseBoolean(profile.getProperty("daemon.continue"));
	}

	@Override
	public Metrics call() throws JobCancelledException {
		logger.info(Log.INIT, "call");
		String myName = this.getClass().getName() + ".call";
		assert profile != null;
		assert config.getNumber() != null;
		setThreadName();
		try {
			super.call();
			if (scanner != null) scanner.rescan();
		} catch (JobCancelledException e) {
			logger.error(Log.ERROR, e.getMessage());
			statusLogger.cancelJob(runKey, e);			
		} catch (SQLException | IOException e) {
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
	
}
