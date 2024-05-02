package sndml.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import sndml.loader.ConfigFactory;
import sndml.loader.ConfigParseException;
import sndml.loader.ConnectionProfile;
import sndml.loader.DateCalculator;
import sndml.util.Log;

public class AppConfigFactory extends ConfigFactory {

	Logger logger = LoggerFactory.getLogger(AppConfigFactory.class);	
	
	public AppConfigFactory() {
		super();
	}

	public AppJobConfig jobConfig(ConnectionProfile profile, JsonNode node) throws ConfigParseException {
		DateCalculator dateFactory = new DateCalculator();
		AppJobConfig config;
		try {
			config = jsonMapper.treeToValue(node, AppJobConfig.class);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e.getMessage());
		}
		config.initialize(profile, dateFactory);
		config.validate();
		logger.debug(Log.INIT, "jobConfig: " + config.toString());
		return config;
	}
	
}
