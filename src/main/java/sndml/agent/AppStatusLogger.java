package sndml.agent;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConnectionProfile;
import sndml.servicenow.*;
import sndml.util.Log;

public class AppStatusLogger {

//	final ConnectionProfile profile;
	final AppSession appSession;
	final URI uriPutRunStatus;
	final URI uriGetRun;
	final Logger logger;
	
	@Deprecated
	public AppStatusLogger(ConnectionProfile profile, AppSession appSession) {
		assert profile != null;
		assert appSession != null;
//		this.profile = profile;
		this.appSession = appSession;
		this.uriPutRunStatus = appSession.getAPI("putrunstatus");
		this.uriGetRun = appSession.getAPI("getrun");
		this.logger = LoggerFactory.getLogger(this.getClass());		
	}
	
	public AppStatusLogger(AppSession appSession) {
		this.appSession = appSession;
		this.uriPutRunStatus = appSession.getAPI("putrunstatus");
		this.uriGetRun = appSession.getAPI("getrun");
		this.logger = LoggerFactory.getLogger(this.getClass());		
		
	}

	public void setStatus(RecordKey runKey, AppJobStatus status) 
			throws IOException, JobCancelledException, IllegalStateException {		
		assert appSession != null;
		assert runKey != null;
		Log.setJobContext(runKey.toString());
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", status.toString().toLowerCase());
		JsonRequest request = new JsonRequest(appSession, uriPutRunStatus, HttpMethod.PUT, body);
		ObjectNode response = request.execute();
		ObjectNode result = (ObjectNode) response.get("result");
		if (!result.has("status")) {
			logger.warn(Log.ERROR, response.toString());
			throw new IllegalStateException("response is missing \"status\" field");
		}
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "setStatus " + runKey + " " + result.toString());
		String newStatus = result.get("status").asText();
		if (!newStatus.equalsIgnoreCase(status.toString()))
			throw new IllegalStateException("Failed to update status. Is there an ACL problem?");
		Log.setGlobalContext();
	}	

	public void logError(RecordKey runKey, Exception e) {
		assert appSession != null;
		assert runKey != null;
		logger.error(Log.PROCESS, "logError " + e.getClass().getSimpleName());
		Log.setJobContext(runKey.toString());
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", "failed");
		body.put("message", e.getMessage());
		JsonRequest request = new JsonRequest(appSession, uriPutRunStatus, HttpMethod.PUT, body);
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
