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
		String myname = this.getClass().getSimpleName() + ".scanUntilDone";
		logger.debug(Log.INIT, String.format("%s begin %s",  myname, agentName));
		boolean done = false;
		while (!done) {
			int jobcount = scan();
			if (jobcount == 0) {
				done = true;				
			}
			else {
				int loopCounter = 0;
				int activeTasks = workerPool.activeTaskCount();
				if (activeTasks == 0) done = true;
				while (activeTasks > 0) {
					// print message every 15 seconds
					if (++loopCounter % 15 == 0)
						logger.info(Log.PROCESS, 
							String.format("scanUntilDone: %d threads running", activeTasks));
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						logger.error(Log.PROCESS, e.getMessage());
						throw e;
					}
					activeTasks = workerPool.activeTaskCount();
				}											
			}
		}
		logger.debug(Log.FINISH, String.format("%s end",  myname));
		if (logger.isDebugEnabled()) workerPool.dumpJobList();
	}

	/**
	 * <p>Submit for execution all jobs that are ready.
	 * Return the number of jobs submitted.</p> 
	 * 
	 * <p>Note: This function does NOT wait for jobs to complete.</p>
	 */
	public int scan() throws IOException, ConfigParseException {
		String myname = this.getClass().getSimpleName() + ".scan";
		Log.setJobContext(agentName);		
		logger.debug(Log.INIT, String.format("%s begin %s",  myname, agentName));
		ArrayList<AppJobRunner> joblist = getJobList();
		if (joblist.size() > 0) {
			// Schedule all jobs for future execution
			// Do not wait for them to complete
			// Each job will open its own Session and Database connection
			for (AppJobRunner job : joblist) {
				workerPool.submit(job);	
			}				
		}
		Log.setGlobalContext();
		int result = joblist.size();
		logger.debug(Log.FINISH, String.format("%s end jobs=%d", myname, result));
		return result;
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
