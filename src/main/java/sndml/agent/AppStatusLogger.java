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

	final ConnectionProfile profile;
	final Session appSession;
	final URI uriPutRunStatus;
	final URI uriGetRun;
	final Logger logger;
	
	public static String SCHEDULED = "scheduled";
	public static String READY = "ready";
	public static String PREPARE = "prepare";
	public static String RUNNING = "running";
	public static String COMPLETE = "complete";
	public static String FAILED = "failed";
	public static String CANCELLED = "cancelled";
		
	public AppStatusLogger(ConnectionProfile profile, Session appSession) {
		assert profile != null;
		assert appSession != null;
		this.profile = profile;
		this.appSession = appSession;
		this.uriPutRunStatus = profile.getAPI("putrunstatus");
		this.uriGetRun = profile.getAPI("getrun");
		this.logger = LoggerFactory.getLogger(this.getClass());		
	}

	public void setStatus(RecordKey runKey, String status) 
			throws IOException, JobCancelledException, IllegalStateException {		
		assert appSession != null;
		assert runKey != null;
		Log.setJobContext(runKey.toString());
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", status);
		JsonRequest request = new JsonRequest(appSession, uriPutRunStatus, HttpMethod.PUT, body);
		ObjectNode response = request.execute();
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "setStatus " + runKey + " " + response.toString());
		String newStatus = response.get("status").asText();
		if (newStatus != status)
			throw new IllegalStateException("Failed to update status. Is there an ACL problem?");
		Log.setGlobalContext();
	}	

//	String getStatus(RecordKey runKey) throws IOException {
//		ObjectNode result = getRun();
//		return result.get("status").asText();
//	}
	
//	ObjectNode getRun() throws IOException, ConfigParseException {
//		JsonRequest request = new JsonRequest(appSession, uriGetRun, HttpMethod.GET, null);
//		ObjectNode response = request.execute();
//		ObjectNode objResult = (ObjectNode) response.get("result");
//		return objResult;
//	}


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
