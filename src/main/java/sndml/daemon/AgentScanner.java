package sndml.daemon;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.TimerTask;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.*;
import sndml.servicenow.*;

public class AgentScanner extends TimerTask {

	final ConnectionProfile profile;
	final Session session;
	final WorkerPool workerPool;
	final AppStatusLogger statusLogger;
	final String agentName;
	final URI uriGetRunList;
	final URI uriPutRunStatus;
	
	final Logger logger = LoggerFactory.getLogger(AgentScanner.class);
	final ConfigFactory configFactory = new ConfigFactory();
	
	final static public String THREAD_NAME = "scanner";
	
	/**
	 * Class that can be run once or run periodically from a timer.
	 * It finds all the jobs that are ready to run, and either runs them immediately (if workerPool is null)
	 * or schedules them to be run by workerPool.
	 * @param profile - Used to establish ServiceNow and database connections.
	 * @param workerPool - If null then jobs will be run sequentially in this thread.
	 */
	AgentScanner(ConnectionProfile profile, WorkerPool workerPool) {
		this.profile = profile;
		this.workerPool = workerPool; // null if single threaded
		this.agentName = AgentDaemon.getAgentName();
		Log.setJobContext(agentName);		
		this.session = profile.getSession();
		assert agentName != null;
		this.uriGetRunList = profile.getAPI("getrunlist", agentName);
		this.uriPutRunStatus = profile.getAPI("putrunstatus");
		this.statusLogger = new AppStatusLogger(profile, session);
		logger.debug(Log.INIT, String.format("workerPool=%s", workerPool));
	}
		
	@Override
	/**
	 * This method is called from the Timer. It performs a single scan and submits
	 * jobs that are "ready". This method is not concerned with other jobs which 
	 * may have a state of "scheduled", since it assumes that recan will be called
	 * as each job completes.
	 */
	public synchronized void run() {
		boolean onExceptionContinue = profile.getPropertyBoolean("daemon.continue", false);
		Log.setJobContext(agentName);		
		try {
			profile.reloadIfChanged();
			scan();
		}
		catch (NoContentException e) {
			logger.error(Log.RESPONSE, String.format(
				"%s encountered %s. Is daemon.agent \"%s\" correct?", 
				uriGetRunList.toString(), e.getClass().getName(), agentName));
			if (!onExceptionContinue) AgentDaemon.abort();
		}		
		catch (IOException | SQLException e) {
			String msg = e.getMessage();
			// Connection resets may happen periodically and should not cause an abort
			boolean connectionReset = msg.toLowerCase().contains("connnection reset");
			logger.error(Log.RESPONSE, e.toString(), e);
			if (!connectionReset && !onExceptionContinue) AgentDaemon.abort();
		}
		catch (Exception e) {
			logger.error(Log.RESPONSE, e.toString(), e);
			AgentDaemon.abort();
			throw e; 
		}
	}
	
	public void scanUntilDone() throws IOException, InterruptedException, ConfigParseException, SQLException {
		logger.debug(Log.INIT, "scanUntilDone");
		if (workerPool == null) {
			int jobcount;
			do { 
				jobcount = scan(); 
			}			
			while (jobcount > 0);
		}
		else {
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
	}
	
	/**
	 * This function is called by {@link AppJobRunner} whenever a job completes.
	 * When a job completes it may cause other jobs to move to a "ready" state.
	 * @throws SQLException 
	 */
	public void rescan() throws ConfigParseException, IOException, SQLException {
		logger.info(Log.PROCESS, "Rescan");
		if (workerPool != null)
			scan();
	}		

	/**
	 * If single threaded, run all jobs that are ready.
	 * If multi-threaded, submit for execution all jobs that are ready.
	 * Return the number of jobs run or submitted.
	 * Note that this function does NOT necessarily wait for all jobs to complete.
	 * @throws SQLException 
	 */
	public int scan() throws ConfigParseException, IOException, SQLException {
		Log.setJobContext(agentName);		
		logger.debug(Log.INIT, "scan");
		ArrayList<AppJobRunner> joblist = getJobList();
		if (joblist.size() > 0) {
			if (workerPool == null) {
				// Use a common ServiceNow Session and Database connection
				Database database = profile.getDatabase();
				// Run the jobs one at a time
				for (AppJobRunner job : joblist) {
					job.setSession(this.session);
					job.setDatabase(database);
					logger.info(Log.INIT, "Running job " + job.number);
					job.run();																					
				}				
			}
			else {
				// Schedule all jobs for future execution
				// Do not wait for them to complete
				// Each job will generate create its own Session and Database connection
				for (AppJobRunner job : joblist) {
					workerPool.execute(job);						
				}				
			}
			Log.setGlobalContext();			
		}
		return joblist.size();
	}

	private ArrayList<AppJobRunner> getJobList() throws ConfigParseException, IOException {
		ArrayList<AppJobRunner> joblist = new ArrayList<AppJobRunner>();
		ArrayNode runlist = getRunList();
		if (runlist != null && runlist.size() > 0) {
			for (JsonNode node : runlist) {
				assert node.isObject();
				RecordKey runKey = new RecordKey(node.get("sys_id").asText());
				String number = node.get("number").asText();
				assert runKey != null;
				assert number != null;
				ObjectNode obj = (ObjectNode) node;
				JobConfig jobConfig = configFactory.jobConfig(profile, obj);
				logger.info(Log.INIT, jobConfig.toString());
				setStatus(runKey, "prepare");
				Log.setJobContext(number);
				AppJobRunner runner = new AppJobRunner(this, profile, jobConfig);
				joblist.add(runner);
			}			
		}
		return joblist;	
	}
	
	private ArrayNode getRunList() throws IOException, ConfigParseException {
		Log.setJobContext(agentName);
		ArrayNode runlist = null;	
		JsonRequest request = new JsonRequest(session, uriGetRunList, HttpMethod.GET, null);
		ObjectNode response = request.execute();
		logger.debug(Log.RESPONSE, response.toPrettyString());
		ObjectNode objResult = (ObjectNode) response.get("result");
		if (objResult.has("runs")) {
			runlist = (ArrayNode) objResult.get("runs");
			if (runlist.size() == 0) { 
				logger.info(Log.INIT, "Nothing ready");
			}
			else { 
				logger.info(Log.INIT, "Runlist=" + getNumbers(runlist));
			}
		}
		return runlist;		
	}
	
	private void setStatus(RecordKey runKey, String status) throws IOException {
		statusLogger.setStatus(runKey, status);
	}	
	
	private String getNumbers(ArrayNode runlist) {		
		ArrayList<String> numbers = new ArrayList<String>();
		for (JsonNode node : runlist) {
			assert node.isObject();
			ObjectNode obj = (ObjectNode) node;
			numbers.add(obj.get("number").asText());
		}
		return String.join(",", numbers);		
	}
		
}
