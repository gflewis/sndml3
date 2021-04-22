package sndml.daemon;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.datamart.ConnectionProfile;
import sndml.servicenow.*;

public class AppStatusLogger {

	final ConnectionProfile profile;
	final Session session;
	final URI putRunStatus;
	final Logger logger;
		
	public AppStatusLogger(ConnectionProfile profile, Session session) {
		assert profile != null;
		assert session != null;
		this.profile = profile;
		this.session = session;
		this.putRunStatus = profile.getAPI("putrunstatus");
		this.logger = LoggerFactory.getLogger(this.getClass());		
	}

	public void setStatus(RecordKey runKey, String status) throws IOException {		
		assert session != null;
		assert runKey != null;
		Log.setJobContext(runKey.toString());
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", status);
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		ObjectNode response = request.execute();
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "setStatus " + runKey + " " + response.toString());
		Log.setGlobalContext();
	}	

	public void logError(RecordKey runKey, Exception e) {
		assert session != null;
		assert runKey != null;
		logger.error(Log.PROCESS, "logError " + e.getClass().getSimpleName());
		Log.setJobContext(runKey.toString());
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", "failed");
		body.put("message", e.getMessage());
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		try {
			request.execute();
		} catch (IOException e1) {
			logger.error(Log.FINISH, "Unable to log Error: " + e.getMessage());
			logger.error(Log.FINISH, "Critical failure. Halting JVM.");
			Runtime.getRuntime().halt(-1);			
		}
		Log.setGlobalContext();
	}
		
}
