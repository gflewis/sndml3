package sndml.daemon;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.TimerTask;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.ConfigFactory;
import sndml.datamart.ConfigParseException;
import sndml.datamart.ConnectionProfile;
import sndml.datamart.JobConfig;
import sndml.servicenow.*;

public class AgentScanner extends TimerTask {

	final ConnectionProfile profile;
	final Session session;
	final WorkerPool workerPool;
	final String agentName;
	final URI uriGetRunList;
	final URI uriPutRunStatus;
	final AppStatusLogger statusLogger;
	final boolean onExceptionContinue;
	
	final Logger logger = LoggerFactory.getLogger(AgentScanner.class);
	final ConfigFactory configFactory = new ConfigFactory();
	
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
		this.session = profile.getSession();
		this.agentName = AgentDaemon.getAgentName();
		assert agentName != null;
		this.uriGetRunList = profile.getAPI("getrunlist", agentName);
		this.uriPutRunStatus = profile.getAPI("putrunstatus");
		this.statusLogger = new AppStatusLogger(profile, session);
		this.onExceptionContinue = profile.getPropertyBoolean("daemon.continue",  false);
		Log.setJobContext(agentName);
		logger.debug(Log.INIT, "workerPool=" + workerPool.toString());
	}
		
	@Override
	public synchronized void run() {
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
		catch (IOException e) {
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

	
	/*
	 * Run the first job in the list until GetRunList returns an empty list.
	 * This function returns when there are no more jobs to run.
	 *
	private int scanWithoutPool() throws IOException, ConfigParseException {
		assert workerPool == null;
		ConfigFactory configFactory = new ConfigFactory();
		int jobCount = 0;
		boolean done = false;		
		while (!done) {
			ArrayNode runlist = getRunList();
			if (runlist == null || runlist.size() == 0) {
				done = true;
			}
			else {
				// Run the first job in the list
				JsonNode node = runlist.get(0);
				assert node.isObject();
				RecordKey runKey = new RecordKey(node.get("sys_id").asText());
				String number = node.get("number").asText();
				assert runKey != null;
				assert number != null;
				ObjectNode obj = (ObjectNode) node;
				JobConfig jobConfig = configFactory.jobConfig(profile, obj);
				Log.setJobContext(number);
				logger.info(Log.INIT, jobConfig.toString());
				setStatus(runKey, "prepare");
				AppJobRunner runner = new AppJobRunner(this, profile, jobConfig);
				logger.info(Log.INIT, "Running job " + number);
				runner.run();
			}				
		}
		return jobCount;
	}

	/**
	 * Submit all jobs for execution by the worker pool.
	 * This function will not wait for jobs to complete.
	 * Each job will call rescan as it completes, to determine if additional jobs need to be submitted.
	 *
	private int scanWithPool_old() throws ConfigParseException, IOException  {
		assert workerPool != null;
		Log.setJobContext(agentName);
		ConfigFactory configFactory = new ConfigFactory();
		int jobCount = 0;
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
				AppJobRunner runner = new AppJobRunner(this, profile, jobConfig);
				workerPool.execute(runner);
				jobCount += 1;				
			}
		}
		return jobCount;	
	}
	*/
	
	/**
	 * If single threaded, run all jobs that are ready.
	 * If multi-threaded, submit for execution all jobs that are ready.
	 * Return the number of jobs run or submitted.
	 * Note that this function does NOT necessarily wait for all jobs to complete.
	 */
	public int scan() throws ConfigParseException, IOException {
		logger.debug(Log.INIT, "scan");
		ArrayList<AppJobRunner> joblist = getJobList();
		for (AppJobRunner job : joblist) {
			if (workerPool == null) {
				// no worker pool; run the job
				logger.info(Log.INIT, "Running job " + job.number);
				job.run();
			}
			else {
				// schedule the job for future execution
				workerPool.execute(job);				
			}
		}
		return joblist.size();
	}

	public void scanUntilDone() throws IOException, InterruptedException {
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
	 */
	public void rescan() throws ConfigParseException, IOException {
		logger.info(Log.PROCESS, "Rescan");
		if (workerPool != null)
			scan();
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
				AppJobRunner runner = new AppJobRunner(this, profile, jobConfig);
				joblist.add(runner);
			}			
		}
		return joblist;	
	}
	
	private ArrayNode getRunList() throws IOException, ConfigParseException {
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
