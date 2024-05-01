package sndml.agent;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.*;
import sndml.servicenow.*;
import sndml.util.Log;

// TODO: Is this class even useful?
class AgentJobModel {

	final Session appSession;
	final ConnectionProfile profile;
	final ConfigFactory configFactory = new ConfigFactory();	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public AgentJobModel(Session appSession) {
		this.appSession = appSession;
		this.profile = Main.getProfile();		
	}
	
	JobConfig fetch(RecordKey jobKey) throws IOException {
		URI uriGetRun = profile.getAPI("getrun", jobKey.toString());		
		JsonRequest request = new JsonRequest(appSession, uriGetRun, HttpMethod.GET, null);
		ObjectNode response = request.execute();
		logger.debug(Log.RESPONSE, response.toPrettyString());
		ObjectNode objResult = (ObjectNode) response.get("result");
		JobConfig config = configFactory.jobConfig(profile, objResult);
		return config;		
	}

}