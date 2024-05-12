package sndml.agent;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigParseException;
import sndml.loader.ConnectionProfile;
import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.RecordKey;
import sndml.util.Log;
import sndml.util.Metrics;

/**
 * A class to execute a single agent job and then terminates.
 */
public class SingleJobRunner implements Runnable {

	final ConnectionProfile profile;
	final AppSession appSession;
	final String agentName;	
	final RecordKey jobKey;
	final AppJobConfig jobConfig;
	final URI uriGetRun;
	final AppConfigFactory configFactory = new AppConfigFactory();
	final Logger logger = LoggerFactory.getLogger(SingleJobRunner.class);
	Metrics metrics;
	
	/**
	 * 
	 * @param profile
	 * @param jobKey
	 * @throws ConfigParseException
	 * @throws IOException
	 * @throws IllegalStateException Job was found but the state was not READY
	 */
	public SingleJobRunner(ConnectionProfile profile, RecordKey jobKey) 
			throws ConfigParseException, IOException, IllegalStateException {
		this.profile = profile;
		this.jobKey = jobKey;
		this.appSession = profile.newAppSession();		
		this.uriGetRun = appSession.getAPI("getrun", jobKey.toString());
		this.agentName = appSession.getAgentName();
		this.jobConfig = configFactory.jobConfig(profile, getRun());
		logger.info(Log.INIT, jobConfig.toString());
		logger.info(Log.INIT, String.format(
				"%s status=%s",jobConfig.number, jobConfig.status.toString()));		
		if (!jobConfig.status.equals(AppJobStatus.READY))
			throw new IllegalStateException(String.format(
					"%s has unexpected Status \"%s\"", 
					jobConfig.number, jobConfig.status.toString()));
		logger.info(Log.INIT, "end constructor");		
	}

	static AppJobConfig getAgentJobRunnerConfig(ConnectionProfile profile, RecordKey jobKey) {
		throw new UnsupportedOperationException("not yet implemented");	
	}
	
	AppSession getAppSession() {
		return appSession;
	}
	
	ObjectNode getRun() throws IOException, ConfigParseException {
		Log.setJobContext(agentName);
		JsonRequest request = new JsonRequest(appSession, uriGetRun, HttpMethod.GET, null);
		logger.info(uriGetRun.toString());
		ObjectNode response = request.execute();
		logger.debug(Log.RESPONSE, response.toPrettyString());
		ObjectNode objResult = (ObjectNode) response.get("result");
		return objResult;
	}

	@Override
	public void run() {
		try {
			AppJobRunner jobRunner = new AppJobRunner(null, profile, jobConfig);
			metrics = jobRunner.call();
			jobRunner.close();
			logger.info(Log.FINISH, metrics.toString());
		} catch (JobCancelledException e) {
			// Throw an unchecked exception which should abort the process.
			// (We are done anyway)
			throw new RuntimeException(e);
		}
				
	}
	
	
}
