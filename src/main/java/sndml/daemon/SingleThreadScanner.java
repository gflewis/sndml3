package sndml.daemon;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import sndml.datamart.*;
import sndml.servicenow.*;

/**
 * An Agent Scanner which does not utilize a thread pool and runs all jobs in the current thread
 * utilizing the current Session and a single database connection.
 */
public class SingleThreadScanner extends AgentScanner {
	
	public SingleThreadScanner(ConnectionProfile profile) {
		super(profile);
	}

	@Override
	protected int getErrorLimit() {
		return 0;
	}
	
	@Override
	public void scanUntilDone() throws IOException, ConfigParseException, SQLException {
		logger.debug(Log.INIT, "scanUntilDone");
		int jobcount;
		do { 
			jobcount = scan(); 
		}			
		while (jobcount > 0);
	}

	/**
	 * Run all jobs that are ready. Return the number of jobs run.
	 * Note that this function there may be new jobs ready to run, 
	 * but his function will not run them.  
	 * @throws SQLException 
	 */
	@Override
	public int scan() throws IOException, ConfigParseException, SQLException {
		Log.setJobContext(agentName);		
		logger.debug(Log.INIT, "scan");
		ArrayList<AppJobRunner> joblist = getJobList();
		if (joblist.size() > 0) {
			// Use a single database connection for all the jobs
			Database database = profile.getDatabase();
			// Run the jobs one at a time
			for (AppJobRunner job : joblist) {
				logger.info(Log.INIT, "Running job " + job.number);
				job.setSession(session);
				job.setDatabase(database);
				job.run();																					
			}				
			Log.setGlobalContext();			
		}
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
