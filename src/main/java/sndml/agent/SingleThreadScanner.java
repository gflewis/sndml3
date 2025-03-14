package sndml.agent;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import sndml.loader.ConfigParseException;
import sndml.loader.Resources;
import sndml.util.Log;

/**
 * An Agent Scanner which does not utilize a thread pool and runs all jobs in the current thread.
 */
public class SingleThreadScanner extends AgentScanner {
	
	public SingleThreadScanner(Resources resources) {
		super(resources);
	}

	@Override
	protected int getErrorLimit() {
		return 0;
	}
	
	@Override
	public void scanUntilDone() throws IOException, ConfigParseException, SQLException {
		String myname = this.getClass().getSimpleName() + ".scanUntilDone";
		logger.debug(Log.INIT, String.format("%s begin %s",  myname, agentName));
		int jobcount;
		do { 
			jobcount = scan(); 
		}			
		while (jobcount > 0);
		logger.debug(Log.FINISH, String.format("%s end",  myname));
	}

	/**
	 * Run all jobs that are ready. Return the number of jobs run.
	 * Note that when this function exits there may be new jobs ready to run, 
	 * but his function will not run them.  
	 * @throws SQLException 
	 */
	@Override
	public int scan() throws IOException, ConfigParseException, SQLException {
		String myname = this.getClass().getSimpleName() + ".scan";
		Log.setJobContext(agentName);		
		logger.debug(Log.INIT, String.format("%s begin %s",  myname, agentName));
		ArrayList<AppJobRunner> joblist = getJobList();
		if (joblist.size() > 0) {
			// Run the jobs one at a time
			for (AppJobRunner job : joblist) {
				logger.info(Log.INIT, "Running job " + job.number);
				job.call();																					
			}				
			Log.setGlobalContext();			
		}
		int result = joblist.size();
		logger.debug(Log.FINISH, String.format("%s end %d", myname, result));
		return result;
		
	}
	
}
