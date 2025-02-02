package sndml.agent;

import java.io.IOException;
import java.util.ArrayList;

import sndml.loader.*;
import sndml.util.Log;

public class MultiThreadScanner extends AgentScanner {

	final WorkerPool workerPool;
	
	public MultiThreadScanner(Resources resources, WorkerPool workerPool) {
		super(resources);
		this.workerPool = workerPool;
	}
	
	@Override
	protected int getErrorLimit() {
		// Allow getrunlist to fail two times in a row, but not three
		return 3;
	}
	
	@Override
	protected AppJobRunner createJob(AppJobConfig jobConfig) {
		Resources workerResources = resources.workerCopy();
		AppJobRunner job = new ScannerJobRunner(this, workerResources, jobConfig);
		return job;
	}		
	
	@Override
	public void scanUntilDone() throws IOException, InterruptedException, ConfigParseException {
		logger.debug(Log.INIT, "scanUntilDone");
		scan();
		int loopCounter = 0;
		while (workerPool.activeTaskCount() > 0) {
			// print message every 15 seconds
			if (++loopCounter % 15 == 0)
				logger.info(Log.PROCESS, 
					String.format("scanUntilDone: %d threads running", workerPool.activeTaskCount()));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error(Log.PROCESS, e.getMessage());
				throw e;
			}	
		}	
	}

	/**
	 * <p>Submit for execution all jobs that are ready.
	 * Return the number of jobs run or submitted.</p> 
	 * 
	 * <p>Note: This function does NOT necessarily wait for all jobs to complete.</p>
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
				workerPool.submit(job);	
			}				
		}
		Log.setGlobalContext();			
		return joblist.size();
	}

//	/**
//	 * This function is called by {@link ScannerJobRunner} whenever a job completes.
//	 * When a job completes it may cause other jobs to move to a "ready" state.
//	 */	
//	@Override
//	public void rescan() throws ConfigParseException, IOException, SQLException {
//		logger.info(Log.PROCESS, "Rescan");
//		scan();
//	}
		
}
