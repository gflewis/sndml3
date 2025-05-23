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
	public void scanUntilDone() throws IOException, ConfigParseException, InterruptedException {
		String myname = this.getClass().getSimpleName() + ".scanUntilDone";
		logger.debug(Log.INIT, String.format("%s begin %s",  myname, agentName));
		boolean done = false;
		while (!done) {
			int jobcount = scanOnce();
			if (jobcount == 0) {
				done = true;				
			}
			else {
				// wait for workerPool to become idle
				int loopCounter = 0;
				int activeTasks = workerPool.activeTaskCount();
				if (activeTasks == 0) done = true;
				while (activeTasks > 0) {
					// print message every 20 seconds
					if (++loopCounter % 20 == 0)
						logger.info(Log.PROCESS, 
							String.format("scanUntilDone: %d active workers", activeTasks));
					Thread.sleep(1000);
					activeTasks = workerPool.activeTaskCount();
				}
				logger.info(Log.PROCESS, "scanUntilDone: no active workers");
				Thread.sleep(rescanDelayMillisec);
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
	
	@Override
	public int scanOnce() throws IOException, ConfigParseException {
		String myname = this.getClass().getSimpleName() + ".scanOnce";
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
		
}
