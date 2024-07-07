package sndml.agent;

import java.io.IOException;
import java.sql.SQLException;

import sndml.loader.ConnectionProfile;
import sndml.util.Log;
import sndml.util.Metrics;

/**
 * An AppJobRunner which performs a re-scan after a job is complete
 */
public class ScannerJobRunner extends AppJobRunner {
	
	final AgentScanner scanner; // my parent
	

	public ScannerJobRunner(AgentScanner scanner, ConnectionProfile profile, AppJobConfig config) {
		super(profile, config);
		this.scanner = scanner;
	}

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
	
}
