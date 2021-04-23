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

public abstract class AgentScanner extends TimerTask {

	final ConnectionProfile profile;
	final Session session;
	final AppStatusLogger statusLogger;
	final String agentName;
	final URI uriGetRunList;
	final URI uriPutRunStatus;
	int errorCount = 0;
	
	final Logger logger = LoggerFactory.getLogger(AgentScanner.class);
	final ConfigFactory configFactory = new ConfigFactory();
	
	final static public String THREAD_NAME = "scanner";
	final static int ERROR_LIMIT = 3;
	
	/**
	 * Class that can be run once or run periodically from a timer.
	 * It finds all the jobs that are ready to run, and either runs them immediately (if workerPool is null)
	 * or schedules them to be run by workerPool.
	 * @param profile - Used to establish ServiceNow and database connections.
	 * @param workerPool - If null then jobs will be run sequentially in this thread.
	 */
	AgentScanner(ConnectionProfile profile) {
		this.profile = profile;
		this.agentName = AgentDaemon.getAgentName();
		Log.setJobContext(agentName);		
		this.session = profile.getSession();
		assert agentName != null;
		this.uriGetRunList = profile.getAPI("getrunlist", agentName);
		this.uriPutRunStatus = profile.getAPI("putrunstatus");
		this.statusLogger = new AppStatusLogger(profile, session);
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
			logger.error(Log.ERROR, "run: " + e.getClass().getName());
			logger.error(Log.ERROR, String.format(
				"%s encountered %s. Is daemon.agent \"%s\" correct?", 
				uriGetRunList.toString(), e.getClass().getName(), agentName));
			if (!onExceptionContinue) AgentDaemon.abort();
		}		
		catch (IOException | SQLException e) {
			logger.error(Log.ERROR, "run: " + e.getClass().getName());
			// Connection resets may happen periodically and should not cause an abort
			// Now handled in getjoblist
			// String msg = e.getMessage();
			// boolean connectionReset = msg.toLowerCase().contains("connnection reset");
			logger.error(Log.ERROR, e.toString(), e);
			// if (!connectionReset && !onExceptionContinue) AgentDaemon.abort();
			if (!onExceptionContinue) AgentDaemon.abort();
		}
		catch (Exception e) {
			logger.error(Log.ERROR, "run: " + e.getClass().getName());
			logger.error(Log.ERROR, e.toString(), e);
			AgentDaemon.abort();
			throw e; 
		}
	}
	
	public abstract void scanUntilDone() throws IOException, InterruptedException, ConfigParseException, SQLException;
	
	public abstract int scan() throws ConfigParseException, IOException, SQLException;

	public abstract void rescan() throws ConfigParseException, IOException, SQLException;
	
	protected abstract int getErrorLimit();
	
	public 	AppJobRunner createJob(JobConfig jobConfig) {
		AppJobRunner job = new AppJobRunner(this, profile, jobConfig);
		return job;
	}	

	ArrayList<AppJobRunner> getJobList() throws IOException, ConfigParseException {
		ArrayList<AppJobRunner> joblist = new ArrayList<AppJobRunner>();
		ArrayNode runlist = null;
		try {
			runlist = getRunList();
			// if getRunList was successful then reset errorCount
			errorCount = 0;
		} catch (IOException e) {
			runlist = null;
			errorCount += 1;
			int errorLimit = getErrorLimit();
			logger.error(Log.ERROR, e.getMessage(), e);
			if (errorCount < errorLimit) {
				session.reset();				
			}
			else {
				// ResourceException will not be caught
				throw new ResourceException(String.format(
					"getrunlist has failed %d times", errorCount));				
			}
		}
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
				// AppJobRunner runner = new AppJobRunner(this, profile, jobConfig);
				AppJobRunner runner = createJob(jobConfig);
				joblist.add(runner);
			}			
		}
		return joblist;	
	}
	
	ArrayNode getRunList() throws IOException, ConfigParseException {
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
	
	void setStatus(RecordKey runKey, String status) throws IOException {
		statusLogger.setStatus(runKey, status);
	}	
	
	String getNumbers(ArrayNode runlist) {		
		ArrayList<String> numbers = new ArrayList<String>();
		for (JsonNode node : runlist) {
			assert node.isObject();
			ObjectNode obj = (ObjectNode) node;
			numbers.add(obj.get("number").asText());
		}
		return String.join(",", numbers);		
	}
		
}
