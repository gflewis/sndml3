package sndml.agent;

import java.io.IOException;
import java.net.URI;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.loader.Resources;
import sndml.servicenow.JsonRequest;
import sndml.util.Log;

public class HeartbeatTask extends TimerTask {
	
	final AppSession appSession;
	final String agentName;
	final URI uri;
	
	final static Logger logger = LoggerFactory.getLogger(HeartbeatTask.class);
	
	public HeartbeatTask(Resources resources) {
		this.appSession = resources.getAppSession();
		this.agentName = resources.getAgentName();		
		this.uri = appSession.getAPI("getrunlist", agentName);
	}

	@Override
	public void run() {
		Log.setGlobalContext();
		JsonRequest request = new JsonRequest(appSession, uri);	
		try {
			request.execute();
		} catch (IOException e) {
			logger.error(Log.REQUEST, e.getMessage(), e);
		}
		logger.info(Log.PROCESS, "heartbeat sent");

	}

}
