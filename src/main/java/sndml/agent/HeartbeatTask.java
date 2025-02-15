package sndml.agent;

import java.io.IOException;
import java.net.URI;
import java.util.TimerTask;

import org.slf4j.Logger;

import sndml.loader.Resources;
import sndml.servicenow.JsonRequest;
import sndml.util.Log;

public class HeartbeatTask extends TimerTask {
	
	final AppSession appSession;
	final String agentName;
	final URI uri;
	
	// TODO If consecutive heartbeatFailures exceeds threshold then abort the program
	// use property server.heartbeat_failure_limit
	int consecutiveFailures = 0;
	
	final static Logger logger = Log.getLogger(HeartbeatTask.class);
	
	public HeartbeatTask(Resources resources) {
		this.appSession = resources.getAppSession();
		this.agentName = resources.getAgentName();		
		this.uri = appSession.uriGetAgent();
	}

	@Override
	public void run() {
		Log.setGlobalContext();
		JsonRequest request = new JsonRequest(appSession, uri);	
		try {
			request.execute();
			consecutiveFailures = 0;
			logger.info(Log.PROCESS, "heartbeat sent");
		} catch (IOException e) {
			consecutiveFailures += 1;
			logger.error(Log.REQUEST, e.getMessage(), e);
			logger.warn(Log.ERROR, String.format("%d heartbeat failure", consecutiveFailures));
		}
	}

}
