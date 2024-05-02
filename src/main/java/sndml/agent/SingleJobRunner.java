package sndml.agent;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigParseException;
import sndml.loader.ConnectionProfile;
import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.Metrics;
import sndml.servicenow.RecordKey;
import sndml.servicenow.Session;
import sndml.util.Log;

/**
 * A class to execute a single agent job and then terminates.
 */
public class SingleJobRunner implements Runnable {

	final ConnectionProfile profile;
	final Session appSession;
	final String agentName;	
	final RecordKey jobKey;
	final AppJobConfig jobConfig;
	final URI uriGetRun;
	final AppConfigFactory configFactory = new AppConfigFactory();
	final Logger logger = LoggerFactory.getLogger(SingleJobRunner.class);
	Metrics metrics;
	
	public SingleJobRunner(ConnectionProfile profile, RecordKey jobKey) 
			throws ConfigParseException, IOException {
		this.profile = profile;
		this.jobKey = jobKey;
		this.appSession = profile.newAppSession();		
		this.uriGetRun = profile.getAPI("getrun", jobKey.toString());
		this.agentName = profile.getAgentName();
		this.jobConfig = configFactory.jobConfig(profile, getRun());
		logger.info(Log.INIT, jobConfig.toString());
		if (!jobConfig.status.equals(AppJobStatus.READY))
			throw new IllegalStateException(String.format(
					"%s has unexpected Status \"%s\" (expected \"READY\");", 
					jobConfig.number, jobConfig.status.toString())); 
		
	}

	static AppJobConfig getAgentJobRunnerConfig(ConnectionProfile profile, RecordKey jobKey) {
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
	public void run() {
		PrintWriter output = new PrintWriter(System.out);
		try {
			AppJobRunner jobRunner = new AppJobRunner(null, profile, jobConfig);
			metrics = jobRunner.call();
			jobRunner.close();
			logger.info(Log.FINISH, metrics.toString());
			metrics.write(output);
		} catch (JobCancelledException e) {
			// Throw an unchecked exception which should abort the process.
			// (We are done anyway)
			throw new RuntimeException(e);
		}
		
		
	}
	
	
}
