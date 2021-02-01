package sndml.datamart;

import java.io.IOException;
import java.net.URI;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.Key;
import sndml.servicenow.Log;
import sndml.servicenow.Session;

public class Scanner extends TimerTask {

	static Logger logger = LoggerFactory.getLogger(Scanner.class);
	
	final ConnectionProfile profile;
	final String name;
	final Session session;
	final Thread mainThread; 
	final ExecutorService workerPool;
	final URI getRunList;
	final URI putRunStatus;
	
	Scanner(ConnectionProfile profile, ExecutorService workerPool) {
		this.profile = profile;
		this.workerPool = workerPool;
		this.mainThread = Thread.currentThread();
		this.session = profile.getSession();
		this.name = profile.getProperty("loader.agent", "main");
		String getRunListPath = profile.getProperty(
			"loader.url.getrunlist", 
			"api/x_108443_sndml/getrunlist/");
		String putRunStatusPath = profile.getProperty(
			"loader.api.putrunstatus",
			"api/x_108443_sndml/putrunstatus");
		this.getRunList = session.getURI(getRunListPath + name);
		this.putRunStatus = session.getURI(putRunStatusPath);
	}
		
	@Override
	public void run() {
		JsonRequest request = new JsonRequest(session, getRunList, HttpMethod.GET, null);
		try {
			ObjectNode response = request.execute();
			logger.info(Log.RESPONSE, response.toPrettyString());
			ObjectNode objResult = (ObjectNode) response.get("result");
			ArrayNode runlist = (ArrayNode) objResult.get("runs");
			for (int i = 0; i < runlist.size(); ++i) {
				ObjectNode obj = (ObjectNode) runlist.get(i);
				JobConfig config = new JobConfig(obj);
				setRunStatus(config.getId(), "prepare");
				ActionRunner runner = new ActionRunner(session, profile, config);
				workerPool.execute(runner);
			}
		} 
		catch (Exception e) {
			logger.error(Log.RESPONSE, e.toString(), e);
			e.printStackTrace();
			mainThread.interrupt();				
		}
	}
	
	private void setRunStatus(Key runKey, String status) throws IOException {
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());
		body.put("status", status);
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		ObjectNode response = request.execute();
		logger.info(Log.RESPONSE, response.toPrettyString());
	}

}
