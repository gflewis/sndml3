package sndml.datamart;

import java.io.IOException;
import java.net.URI;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;

import sndml.servicenow.*;

public class AppRunLogger extends ProgressLogger {

	final Session session;
	final URI putRunStatus;
	Key runKey;
	
	AppRunLogger(Logger logger, ConnectionProfile profile, Session session) {
		super(logger);
		this.session = session;
		String putRunStatusPath = profile.getProperty(
			"loader.api.putrunstatus", 
			"api/x_108443_sndml/putrunstatus");
		this.putRunStatus = session.getURI(putRunStatusPath);		
	}
	
	public AppRunLogger setRunKey(Key runKey) {
		this.runKey = runKey;
		return this;
	}
	
	public void setStatus(String status) throws IOException {
		assert runKey != null;
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());
		body.put("status", status);
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		ObjectNode response = request.execute();
		logger.info(Log.RESPONSE, "key=" + runKey + " " + response.toString());
	}	

}
