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
		String myname = this.getClass().getSimpleName() + ".call";
		logger.info(Log.INIT, myname + " begin");
		assert profile != null;
		assert config.getNumber() != null;
		setThreadName();
		try {
			super.call();
			if (scanner != null) {
				logger.debug(Log.FINISH, myname + " rescan");
				scanner.rescan();
			}
		} catch (JobCancelledException e) {
			logger.error(Log.ERROR, e.getMessage());
			statusLogger.cancelJob(runKey, e);			
		} catch (SQLException | IOException e) {
			Log.setJobContext(this.getName());
			logger.error(Log.ERROR, myname + ": " + e.getClass().getName(), e);
			statusLogger.logError(runKey, e);
			if (!onExceptionContinue) AgentDaemon.abort();			
		} catch (Error e) {
			logger.error(Log.ERROR, myname + ": " + e.getClass().getName(), e);
			logger.error(Log.ERROR, "Critical error detected. Halting JVM.");
			Runtime.getRuntime().halt(-1);
		}
		logger.debug(Log.FINISH, myname + " complete");
		return jobMetrics;
	}
	
}
