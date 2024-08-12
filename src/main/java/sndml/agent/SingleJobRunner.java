package sndml.agent;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigParseException;
import sndml.loader.ConnectionProfile;
import sndml.loader.Resources;
import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.RecordKey;
import sndml.util.Log;
import sndml.util.Metrics;

/**
 * A class to execute a single agent job and then terminates.
 */
@Deprecated
public class SingleJobRunner implements Runnable {

	final Resources resources;
	final ConnectionProfile profile;
	final AppSession appSession;
	final String agentName;	
	final RecordKey jobKey;
	final AppJobConfig jobConfig;
	final URI uriGetRun;
	final AppConfigFactory configFactory;
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
	public SingleJobRunner(Resources resources, RecordKey jobKey) 
			throws ConfigParseException, IOException, IllegalStateException {
		this.resources = resources;
		this.profile = resources.getProfile();
		this.jobKey = jobKey;
		this.appSession = profile.newAppSession();
		this.configFactory = new AppConfigFactory(resources);
		this.uriGetRun = appSession.uriGetJobRunConfig(jobKey);
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
		AppJobRunner jobRunner = null;
		try {
			jobRunner = new AppJobRunner(resources, jobConfig);
			metrics = jobRunner.call();
			jobRunner.close();
			logger.info(Log.FINISH, metrics.toString());
		} catch (Exception e) {
			logger.error(Log.ERROR, e.getMessage(), e);
			if (jobRunner != null) {
				try {
					AppStatusLogger statusLogger = jobRunner.getStatusLogger();
					statusLogger.logError(this.jobKey, e);
				}
				catch (Exception e1) {
					logger.error(Log.ERROR, e1.getMessage(), e1);
				}
			}
			// Throw an unchecked exception which should abort the process.
			// (We are done anyway)
			throw new RuntimeException(e);
		}				
	}
	
	
}
