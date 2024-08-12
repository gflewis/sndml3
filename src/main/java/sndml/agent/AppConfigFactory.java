package sndml.agent;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigFactory;
import sndml.loader.ConfigParseException;
import sndml.loader.ConnectionProfile;
import sndml.loader.DateCalculator;
import sndml.loader.Resources;
import sndml.servicenow.RecordKey;
import sndml.util.Log;

public class AppConfigFactory extends ConfigFactory {

	final Resources resources;
	final AppSession appSession;
	final ConnectionProfile profile;
	final DateCalculator dateCalculator;
	
	Logger logger = Log.getLogger(AppConfigFactory.class);	
	
	public AppConfigFactory(Resources resources) {		
		super();
		this.resources = resources;
		this.appSession = resources.getAppSession();
		this.profile = resources.getProfile();
		this.dateCalculator = new DateCalculator();				
	}

	public AppJobConfig jobConfig(ConnectionProfile profile, JsonNode node) throws ConfigParseException {
		DateCalculator dateFactory = new DateCalculator();
		AppJobConfig config;
		try {
			config = jsonMapper.treeToValue(node, AppJobConfig.class);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e.getMessage());
		}
		config.initializeAndValidate(profile, dateFactory);
		logger.debug(Log.INIT, "jobConfig: " + config.toString());
		return config;
	}
	
	public AppJobConfig appJobConfig(RecordKey runKey) throws ConfigParseException, IOException {
		URI uriGetRun = appSession.uriGetJobRunConfig(runKey);
		ObjectNode node = appSession.httpGet(uriGetRun);		
		AppJobConfig config;
		try {
			config = jsonMapper.treeToValue(node, AppJobConfig.class);
			config.initialize(profile, dateCalculator);
			config.validate();
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e.getMessage());
		}
		return config;		
	}
	
	// TODO can this procedure use AppSession.httpGet ?
//	private ObjectNode getRun(RecordKey jobKey) throws IOException, ConfigParseException {
//		Log.setJobContext(appSession.getAgentName());
//		URI uriGetRun = appSession.uriGetJobRunConfig(jobKey);
//		JsonRequest request = new JsonRequest(appSession, uriGetRun, HttpMethod.GET, null);
//		logger.info(Log.INIT, uriGetRun.toString());
//		ObjectNode response = request.execute();
//		logger.debug(Log.RESPONSE, response.toPrettyString());
//		ObjectNode objResult = (ObjectNode) response.get("result");
//		return objResult;
//	}

}
