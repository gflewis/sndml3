package sndml.daemon;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import sndml.datamart.*;
import sndml.servicenow.*;

public class MultiThreadScanner extends AgentScanner {

	final WorkerPool workerPool;

	public MultiThreadScanner(ConnectionProfile profile, WorkerPool workerPool) {
		super(profile);
		this.workerPool = workerPool;
		logger.debug(Log.INIT, String.format("workerPool=%s", workerPool));
	}
	
	@Override
	protected int getErrorLimit() {
		// Allow getrunlist to fail two times in a row, but not three
		return 3;
	}
	
	@Override
	public void scanUntilDone() throws IOException, InterruptedException, ConfigParseException {
		logger.debug(Log.INIT, "scanUntilDone");
		scan();
		int loopCounter = 0;
		while (workerPool.getActiveCount() > 0) {
			// print message every 15 seconds
			if (++loopCounter % 15 == 0)
				logger.info(Log.PROCESS, 
					String.format("scanUntilDone: %d threads running", workerPool.getActiveCount()));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error(Log.PROCESS, e.getMessage());
				throw e;
			}												
		}	
	}

	/**
	 * Submit for execution all jobs that are ready.
	 * Return the number of jobs run or submitted.
	 * Note that this function does NOT necessarily wait for all jobs to complete.
	 * @throws SQLException 
	 */
	public int scan() throws IOException, ConfigParseException {
		Log.setJobContext(agentName);		
		logger.debug(Log.INIT, "scan");
		ArrayList<AppJobRunner> joblist = getJobList();
		if (joblist.size() > 0) {
			// Schedule all jobs for future execution
			// Do not wait for them to complete
			// Each job will generate create its own Session and Database connection
			for (AppJobRunner job : joblist) {
				workerPool.execute(job);						
			}				
		}
		Log.setGlobalContext();			
		return joblist.size();
	}

	/**
	 * This function is called by {@link AppJobRunner} whenever a job completes.
	 * When a job completes it may cause other jobs to move to a "ready" state.
	 * @throws SQLException 
	 */	
	@Override
	public void rescan() throws ConfigParseException, IOException, SQLException {
		logger.info(Log.PROCESS, "Rescan");
		scan();
	}
		
}
