package sndml.agent;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.servicenow.*;
import sndml.util.Log;
import sndml.util.ResourceException;

public class AppStatusLogger {

	final AppSession appSession;
	final Logger logger = LoggerFactory.getLogger(AppStatusLogger.class);
	
	public AppStatusLogger(AppSession appSession) {
		this.appSession = appSession;
	}

	public synchronized void setStatus(RecordKey runKey, AppJobStatus status) 
			throws IOException, JobCancelledException, IllegalStateException {		
		assert appSession != null;
		assert runKey != null;
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("runkey", runKey.toString());		
		body.put("status", status.toString().toLowerCase());
		ObjectNode result = putRunStatus(runKey, body);
		
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "setStatus " + runKey + " " + result.toString());
		// TODO Remove redundant code (next 2 statements appear in putRunStatus)
		String newStatus = result.get("status").asText();
		if (newStatus.equalsIgnoreCase(AppJobStatus.CANCELLED.toString())) {
			logger.warn(Log.RESPONSE, "Cancel detected");
			throw new JobCancelledException(runKey);
		}
		if (!newStatus.equalsIgnoreCase(status.toString()))
			throw new IllegalStateException("Failed to update status. Is there an ACL problem?");
	}	

	void cancelJob(RecordKey runKey, Exception sourceException) {
		String sourceName = sourceException.getClass().getSimpleName();
		cancelJob(runKey, sourceName);
	}

	void cancelJob(RecordKey runKey, String source) {
		logger.warn(Log.FINISH, String.format(
			"cancelJob %s from %s", 
			runKey.toString(), source));
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("runkey", runKey.toString());		
		body.put("status", AppJobStatus.CANCELLED.toString().toLowerCase());
		URI uriPutJobRun = appSession.uriPutJobRunStatus(runKey);
		JsonRequest request = new JsonRequest(appSession, uriPutJobRun, HttpMethod.PUT, body, runKey);		
		try {
			request.execute();
		} catch (IOException e1) {
			logger.warn(Log.FINISH, "cancelJob: " + e1.getMessage());
		}		
	}
	
	public void logError(RecordKey runKey, Exception e) {
		assert appSession != null;
		assert runKey != null;
		logger.error(Log.PROCESS, "logError " + e.getClass().getSimpleName());
		Log.setJobContext(runKey.toString());
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("runkey", runKey.toString());		
		body.put("status", AppJobStatus.FAILED.toString().toLowerCase());
		body.put("message", e.getMessage());
		try {
			putRunStatus(runKey, body);
		} catch (IOException e1) {
			logger.error(Log.FINISH, "Unable to log Error: " + e.getMessage());
			logger.error(Log.FINISH, "Critical failure. Halting JVM.");
			Runtime.getRuntime().halt(-1);
		}		
		Log.setGlobalContext();
	}
	
	/**
	 * Update status and/or metrics in the JobRun
	 */
	ObjectNode putRunStatus(RecordKey runKey, ObjectNode body) throws JobCancelledException {
		logger.info(Log.REQUEST, String.format(
			"putRunStatus request=%s", body.toString()));
		URI uriPutJobRun = appSession.uriPutJobRunStatus(runKey);
		JsonRequest requestObj = new JsonRequest(appSession, uriPutJobRun, HttpMethod.PUT, body, runKey);		
		ObjectNode responseObj;
		try {
			responseObj = requestObj.execute();
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		ObjectNode responseResult = (ObjectNode) responseObj.get("result");
		logger.debug(Log.REQUEST, String.format(
				"putRunStatus response=%s", responseResult.toString()));
		if (!responseResult.has("status")) {
			logger.warn(Log.ERROR, responseObj.toString());
			throw new IllegalStateException("response is missing \"status\" field");
		}		
		String responseStatus = responseResult.get("status").asText();
		if (responseStatus.equalsIgnoreCase(AppJobStatus.CANCELLED.toString())) {
			logger.warn(Log.RESPONSE, String.format(
					"putRunStatus detected CANCEL %s", runKey.toString()));
			throw new JobCancelledException(runKey);
		}
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, String.format(
				"putRunStatus %s %s", runKey, responseObj.toString()));
		return responseResult;
	}
	
}
