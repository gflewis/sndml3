package sndml.agent;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigFactory;
import sndml.loader.ConfigParseException;
import sndml.loader.ConnectionProfile;
import sndml.loader.DatabaseConnection;
import sndml.loader.JobConfig;
import sndml.loader.JobRunner;
import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.Metrics;
import sndml.servicenow.RecordKey;
import sndml.servicenow.Session;
import sndml.util.Log;

/**
 * A class to execute a single agent job and then terminates.
 */
public class AgentSingle implements Runnable {

	final ConnectionProfile profile;
	final Session appSession;
	final String agentName;	
	final RecordKey jobKey;
	final JobConfig jobConfig;
	final URI uriGetRun;
	final ConfigFactory configFactory = new ConfigFactory();
	final Logger logger = LoggerFactory.getLogger(AgentSingle.class);
	Metrics metrics;
	
	public AgentSingle(ConnectionProfile profile, RecordKey jobKey) 
			throws ConfigParseException, IOException {
		this.profile = profile;
		this.jobKey = jobKey;
		this.appSession = profile.newAppSession();		
		this.uriGetRun = profile.getAPI("getrun", jobKey.toString());
		this.agentName = profile.getAgentName();
		this.jobConfig = configFactory.jobConfig(profile, getRun());
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
	public void run() {
		logger.info(Log.INIT, jobConfig.toString());		
		PrintWriter output = new PrintWriter(System.out);
		try {
			Session session = profile.newReaderSession();
			DatabaseConnection database = profile.newDatabaseConnection();
			JobRunner jobRunner = new JobRunner(session, database, jobConfig);
			metrics = jobRunner.call();
			logger.info(Log.FINISH, metrics.toString());
			metrics.write(output);
		} catch (SQLException | IOException | InterruptedException | JobCancelledException e) {
			// Throw an unchecked exception which should abort the process.
			// (We are done anyway)
			throw new RuntimeException(e);
		}
		
		
	}
	
	
}
