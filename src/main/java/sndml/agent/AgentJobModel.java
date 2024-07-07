package sndml.agent;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.*;
import sndml.servicenow.*;
import sndml.util.Log;

@Deprecated
// Use AppConfigFactory
public class AgentJobModel {

	final AppSession appSession;
	final ConnectionProfile profile;
	final ConfigFactory configFactory = new ConfigFactory();	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public AgentJobModel(AppSession appSession) {
		this.appSession = appSession;
		this.profile = ResourceManager.getProfile();		
	}
	
	public AppJobConfig getConfig(String sys_id) throws IOException {
		return getConfig(new RecordKey(sys_id));
	}
	
	public AppJobConfig getConfig(RecordKey jobKey) throws IOException {
		URI uriGetRun = appSession.getAPI("getrun", jobKey.toString());		
		JsonRequest request = new JsonRequest(appSession, uriGetRun, HttpMethod.GET, null);
		ObjectNode response = request.execute();
		logger.debug(Log.RESPONSE, response.toPrettyString());
		ObjectNode objResult = (ObjectNode) response.get("result");
		AppJobConfig config = (AppJobConfig) configFactory.jobConfig(profile, objResult);
		return config;		
	}

}
