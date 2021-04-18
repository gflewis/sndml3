package sndml.daemon;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

	static Logger logger = LoggerFactory.getLogger(AgentScanner.class);
	
	final ConnectionProfile profile;
	final Session session;
	final ExecutorService workerPool;
	final String agentName;
	final URI uriGetRunList;
	final URI uriPutRunStatus;
	final AppStatusLogger statusLogger;
	final boolean onExceptionContinue;
	
	AgentScanner(ConnectionProfile profile) {
		this.profile = profile;
		this.agentName = profile.getProperty("daemon.agent", "main");
		assert agentName != null;
		Log.setJobContext(agentName);
		int threadCount = profile.getPropertyInt("daemon.threads", 0);
		if (threadCount > 0) {
			BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<Runnable>();
			this.workerPool = new ThreadPoolExecutor(threadCount, threadCount, 60, TimeUnit.SECONDS, blockingQueue);			
		}
		else {
			this.workerPool = null;
		}
		this.session = profile.getSession();
		this.uriGetRunList = profile.getAPI("getrunlist", agentName);
		this.uriPutRunStatus = profile.getAPI("putrunstatus");
		this.statusLogger = new AppStatusLogger(profile, session);
		this.onExceptionContinue = profile.getPropertyBoolean("daemon.continue",  false);		
	}
	
	AgentScanner(ConnectionProfile profile, ExecutorService workerPool) {
		this.profile = profile;
		this.workerPool = workerPool; // null if single threaded
		this.session = profile.getSession();
		this.agentName = DaemonLauncher.getAgentName();
		assert agentName != null;
		this.uriGetRunList = profile.getAPI("getrunlist", agentName);
		this.uriPutRunStatus = profile.getAPI("putrunstatus");
		this.statusLogger = new AppStatusLogger(profile, session);
		this.onExceptionContinue = profile.getPropertyBoolean("daemon.continue",  false);
	}
		
	@Override
	public synchronized void run() {
		try {
			scan();
		}
		catch (NoContentException e) {
			logger.error(Log.RESPONSE, String.format(
				"%s encountered %s. Is daemon.agent \"%s\" correct?", 
				uriGetRunList.toString(), e.getClass().getName(), agentName));
			if (!onExceptionContinue) DaemonLauncher.abort();
		}		
		catch (IOException e) {
			String msg = e.getMessage();
			// Connection resets may happen periodically and should not cause an abort
			boolean connectionReset = msg.toLowerCase().contains("connnection reset");
			logger.error(Log.RESPONSE, e.toString(), e);
			if (!connectionReset && !onExceptionContinue) DaemonLauncher.abort();
		}
		catch (Exception e) {
			logger.error(Log.RESPONSE, e.toString(), e);
			DaemonLauncher.abort();
			throw e; 
		}
	}

	public int scan() throws IOException, ConfigParseException {
		if (workerPool == null)
			return scanWithoutPool();
		else
			return scanWithPool();
	}
	
	/**
	 * Run the first job in the list until GetRunList returns an empty list.
	 */
	public int scanWithoutPool() throws IOException, ConfigParseException {
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
				AppJobRunner runner = new AppJobRunner(profile, jobConfig);
				logger.info(Log.INIT, "Running job " + number);
				runner.run();
			}				
		}
		return jobCount;
	}

	/**
	 * Submit all jobs for execution by the worker pool.
	 */
	public int scanWithPool() throws IOException, ConfigParseException {
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
				AppJobRunner runner = new AppJobRunner(profile, jobConfig);
				workerPool.execute(runner);
				jobCount += 1;				
			}
		}
		return jobCount;
	
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
