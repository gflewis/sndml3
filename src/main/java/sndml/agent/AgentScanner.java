package sndml.agent;

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

import sndml.loader.*;
import sndml.servicenow.*;
import sndml.util.Log;
import sndml.util.ResourceException;

public abstract class AgentScanner extends TimerTask {

	final ConnectionProfile profile;
	final AppSession appSession;
	final AppConfigFactory configFactory;	
	final AppStatusLogger statusLogger;
	final String agentName;
	final URI uriGetRunList;
	final URI uriPutRunStatus;
	int errorCount = 0;
	
	final Logger logger = LoggerFactory.getLogger(AgentScanner.class);
	
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
		this.appSession = profile.newAppSession();
		this.configFactory = new AppConfigFactory(appSession);
		assert agentName != null;
		this.uriGetRunList = appSession.getAPI("getrunlist", agentName);
		this.uriPutRunStatus = appSession.getAPI("putrunstatus");
		this.statusLogger = new AppStatusLogger(appSession);
//		SchemaFactory.setSchemaReader(new AppSchemaReader(appSession));		
	}
		
	@Override
	/**
	 * This method is called from the Timer. It performs a single scan and submits
	 * jobs that are "ready". This method is not concerned with other jobs which 
	 * may have a state of "scheduled", since it assumes that recan will be called
	 * as each job completes.
	 */
	public synchronized void run() {
		boolean onExceptionContinue = profile.app.getBoolean("continue", false);
		Log.setJobContext(agentName);		
		try {
			scan();
		}
		catch (NoContentException e) {
			logger.error(Log.ERROR, "run: " + e.getClass().getName());
			logger.error(Log.ERROR, String.format(
				"%s encountered %s. Is agent \"%s\" correct?", 
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
	
	public abstract void scanUntilDone() 
			throws IOException, InterruptedException, ConfigParseException, SQLException;
	
	public abstract int scan() throws ConfigParseException, IOException, SQLException;

	public abstract void rescan() throws ConfigParseException, IOException, SQLException;
	
	protected abstract int getErrorLimit();
	
	public 	AppJobRunner createJob(AppJobConfig jobConfig) {
		AppJobRunner job = new ScannerJobRunner(this, profile, jobConfig);
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
				appSession.reset();				
			}
			else {
				// ResourceException will not be caught
				throw new ResourceException(String.format(
					"getrunlist has failed %d times", errorCount));				
			}
		}
		if (runlist != null && runlist.size() > 0) {
			for (JsonNode node : runlist) {
				boolean cancelDetected = false;
				assert node.isObject();
				RecordKey runKey = new RecordKey(node.get("sys_id").asText());
				String number = node.get("number").asText();
				assert runKey != null;
				assert number != null;
				ObjectNode obj = (ObjectNode) node;
				AppJobConfig jobConfig = configFactory.jobConfig(profile, obj);
				logger.info(Log.INIT, jobConfig.toString());
				try {
					setStatus(runKey, AppJobStatus.PREPARE);
				} catch (JobCancelledException e) {
					logger.warn(Log.FINISH, "Job Cancel Detected");
					cancelDetected = true;
				}
				if (!cancelDetected) {
					Log.setJobContext(number);
					AppJobRunner runner = createJob(jobConfig);
					joblist.add(runner);					
				}
			}			
		}
		return joblist;	
	}
	
	ArrayNode getRunList() throws IOException, ConfigParseException {
		Log.setJobContext(agentName);
		GetRunListRequest request = new GetRunListRequest(appSession, agentName);
		ArrayNode runlist = request.getRunList();
		if (runlist == null || runlist.size() == 0) {
			logger.info(Log.INIT, "Nothing ready");			
		}
		else {
			logger.info(Log.INIT, "Runlist=" + getNumbers(runlist));			
		}		
		return runlist;		
	}
	
	void setStatus(RecordKey runKey, AppJobStatus status) throws JobCancelledException, IOException {
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
	
	/**
	 * This function can be called by any thread to abort the scanner.
	 */
	public void abort() {
		logger.error(Log.FINISH, "Aborting the scanner");
		Runtime.getRuntime().exit(-1);
	}
	
		
}
