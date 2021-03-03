package sndml.datamart;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class Scanner extends TimerTask {

	static Logger logger = LoggerFactory.getLogger(Scanner.class);
	
	final ConnectionProfile profile;
	final String agentName;
	final Session session;
	final ExecutorService workerPool;
	final URI getRunList;
	final URI putRunStatus;
	
	Scanner(ConnectionProfile profile, ExecutorService workerPool) {
		this.profile = profile;
		this.workerPool = workerPool;
		this.session = profile.getSession();
		// this.statusLogger = new AppRunLogger(logger, profile, session);
		this.agentName = profile.getProperty("loader.agent", "main");
		String getRunListPath = profile.getProperty(
			"loader.api.getrunlist", 
			"api/x_108443_sndml/getrunlist/");
		String putRunStatusPath = profile.getProperty(
			"loader.api.putrunstatus",
			"api/x_108443_sndml/putrunstatus");
		this.getRunList = session.getURI(getRunListPath + agentName);
		this.putRunStatus = session.getURI(putRunStatusPath);
	}
		
	@Override
	public synchronized void run() {
		Log.setJobContext(agentName);
		ConfigFactory configFactory = new ConfigFactory(DateTime.now());
		JsonRequest request = new JsonRequest(session, getRunList, HttpMethod.GET, null);
		try {
			ObjectNode response = request.getObject();
			logger.debug(Log.RESPONSE, response.toPrettyString());
			ObjectNode objResult = (ObjectNode) response.get("result");
			if (objResult.has("runs")) {
				ArrayNode runlist = (ArrayNode) objResult.get("runs");
				if (runlist.size() == 0) { 
					logger.info(Log.DAEMON, "No Runs");
				}
				else { 
					logger.info(Log.DAEMON, "Runlist=" + getNumbers(runlist));
				}
				for (JsonNode node : runlist) {
					assert node.isObject();
					Key runKey = new Key(node.get("sys_id").asText());
					String number = node.get("number").asText();
					assert runKey != null;
					assert number != null;
					AppRunLogger statusLogger = new AppRunLogger(profile, session, number, runKey);
					ObjectNode obj = (ObjectNode) node;
					try {
						JobConfig jobConfig = configFactory.jobConfig(profile, obj);
						logger.info(Log.DAEMON, jobConfig.toString());
						statusLogger.setStatus("prepare");
						DaemonJobRunner runner = new DaemonJobRunner(profile, jobConfig);
						workerPool.execute(runner);
					}
					catch (ConfigParseException e) {
						logger.error(Log.RESPONSE, e.toString(), e);
						statusLogger.logError(e);
					}
				}
			}
			else
				logger.info(Log.DAEMON, "No Runs");
		} 
		catch (IOException e) {
			logger.error(Log.RESPONSE, e.toString(), e);
			Daemon.getThread().interrupt();
		}
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
