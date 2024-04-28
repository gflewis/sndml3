package sndml.agent;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigFactory;
import sndml.loader.ConfigParseException;
import sndml.loader.ConnectionProfile;
import sndml.loader.JobConfig;
import sndml.loader.JobRunner;
import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.Metrics;
import sndml.servicenow.RecordKey;
import sndml.servicenow.Session;
import sndml.util.Log;

public class AgentJobRunner implements Callable<Metrics> {

	final ConnectionProfile profile;
	final Session appSession;
	final String agentName;	
	final RecordKey jobKey;
	final JobConfig jobConfig;
	final JobRunner jobRunner;
	final URI uriGetRun;
	final ConfigFactory configFactory = new ConfigFactory();
	final Logger logger = LoggerFactory.getLogger(AgentJobRunner.class);
	
	public AgentJobRunner(ConnectionProfile profile, RecordKey jobKey) 
			throws ConfigParseException, IOException {
		this.profile = profile;
		this.jobKey = jobKey;
		this.appSession = profile.getAppSession();		
		this.uriGetRun = profile.getAPI("getrun", jobKey.toString());
		this.agentName = profile.getAgentName();
		this.jobConfig = configFactory.jobConfig(profile, getRun());
		this.jobRunner = new JobRunner(profile, jobConfig);
	}

	static JobConfig getAgentJobRunnerConfig(ConnectionProfile profile, RecordKey jobKey) {
		throw new UnsupportedOperationException("not yet implemented");	
	}

	ObjectNode getRun() throws IOException, ConfigParseException {
		Log.setJobContext(agentName);
		JsonRequest request = new JsonRequest(appSession, uriGetRun, HttpMethod.GET, null);
		ObjectNode response = request.execute();
		logger.debug(Log.RESPONSE, response.toPrettyString());
		ObjectNode objResult = (ObjectNode) response.get("result");
		return objResult;
	}

	@Override
	public Metrics call() throws Exception {
		return jobRunner.call();
	}
	
	
}
