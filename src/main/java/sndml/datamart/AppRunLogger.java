package sndml.datamart;

import java.io.IOException;
import java.net.URI;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class AppRunLogger extends ProgressLogger {

	final Session session;
	final URI putRunStatus;
	final Key runKey;
	final Logger logger = LoggerFactory.getLogger(AppRunLogger.class);
	
	AppRunLogger(ConnectionProfile profile, Session session, Key runKey) {
		super();
		this.session = session;
		this.runKey = runKey;
		String putRunStatusPath = profile.getProperty(
			"loader.api.putrunstatus", 
			"api/x_108443_sndml/putrunstatus");
		this.putRunStatus = session.getURI(putRunStatusPath);		
	}
	
	public void setStatus(String status) throws IOException {
		assert runKey != null;
		logger.info(Log.REQUEST, String.format("setStatus %s %s", runKey, status));
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", status);
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		ObjectNode response = request.execute();
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "setStatus " + runKey + " " + response.toString());
	}	
	
	public void logError(Exception e) {
		assert runKey != null;
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", "failed");
		body.put("message", e.getMessage());
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		try {
			request.execute();
		} catch (IOException e1) {
			logger.error(Log.FINISH, "Unable to log Error: " + e.getMessage());
		}		
	}
	
	@Override
	public void logProgress(ReaderMetrics readerMetrics, WriterMetrics writerMetrics) {
		assert runKey != null;
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", "running");
		body.put("expected", readerMetrics.getExpected());
		body.put("inserted",  writerMetrics.getInserted());
		body.put("updated",  writerMetrics.getUpdated());
		body.put("deleted",  writerMetrics.getDeleted());
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		ObjectNode response;
		try {
			response = request.execute();
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "logProgress " + runKey + " " + response.toString());
	}
	

}
