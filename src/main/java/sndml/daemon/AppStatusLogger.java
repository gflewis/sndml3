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
		this.profile = profile;
		this.session = session;
		this.putRunStatus = AppDaemon.getAPI(session, "putrunstatus");
		this.logger = LoggerFactory.getLogger(this.getClass());		
	}

	public void setStatus(Key runKey, String status) throws IOException {
		assert runKey != null;
		Log.setJobContext(runKey.toString());
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", status);
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		ObjectNode response = request.execute();
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "setStatus " + runKey + " " + response.toString());
	}	

	public void logError(Key runKey, Exception e) {
		assert runKey != null;
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
	}
		
}
