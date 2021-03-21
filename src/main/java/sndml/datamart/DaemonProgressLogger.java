package sndml.datamart;

import java.io.IOException;
import java.net.URI;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class DaemonProgressLogger extends ProgressLogger {

	final ConnectionProfile profile;
	final Session session;
	final URI putRunStatusURI;
	final String number;
	final Key runKey;
	final static Logger logger = LoggerFactory.getLogger(DaemonProgressLogger.class);	

	DaemonProgressLogger(
			ConnectionProfile profile, 
			Session session,
			Metrics metrics,
			String number, 
			Key runKey) {
		this(profile, session, metrics, number, runKey, null);
	}
	
	DaemonProgressLogger(
			ConnectionProfile profile, 
			Session session,
			Metrics metrics,
			String number, 
			Key runKey,
			DatePart part) {
		super(metrics, part);
		assert runKey != null;
		this.profile = profile;
		this.session = session;
		this.number = number;
		this.runKey = runKey;
		String putRunStatusPath = profile.getProperty(
			"loader.api.putrunstatus", 
			"api/x_108443_sndml/putrunstatus");
		this.putRunStatusURI = session.getURI(putRunStatusPath);		
		// logger.info(Log.INIT, "DaemonProgressLogger " + number);
	}

	@Override
	public ProgressLogger newPartLogger(Metrics newMetrics, DatePart newPart) {
		// logger.info(Log.INIT, "newPartLogger");
		return new DaemonProgressLogger(
			this.profile, this.session, newMetrics, this.number, this.runKey, newPart);
	}
	
	@Override
	public void logPrepare() {
		putRunStatus(messageBody("prepare"));
	}

	@Override
	public void logStart(Integer expected) {	
		// logger.info(Log.INIT, String.format("logProgress %d", expected));
		ObjectNode body = messageBody("running");
		String fieldname = hasPart() ? "part_expected" : "expected";
		body.put(fieldname,  expected);
		putRunStatus(body);
	}
		
	@Override
	public void logProgress() {
		// logger.info(Log.PROCESS, "logProgress");
		assert metrics != null;
		ObjectNode body = messageBody("running");
		if (hasPart()) {
			assert metrics.hasParent();
			body.put("expected", metrics.getParent().getExpected());
			body.put("inserted",  metrics.getParent().getInserted());
			body.put("updated",  metrics.getParent().getUpdated());
			body.put("deleted",  metrics.getParent().getDeleted());			
			body.put("part_expected", metrics.getExpected());
			body.put("part_inserted",  metrics.getInserted());
			body.put("part_updated",  metrics.getUpdated());
			body.put("part_deleted",  metrics.getDeleted());						
		}
		else {
			body.put("expected", metrics.getExpected());
			body.put("inserted",  metrics.getInserted());
			body.put("updated",  metrics.getUpdated());
			body.put("deleted",  metrics.getDeleted());			
		}
		putRunStatus(body);
	}

	@Override
	public void logComplete() {
		// logger.info(Log.FINISH, "logComplete");
		ObjectNode body = messageBody("complete");
		putRunStatus(body);
	}

	ObjectNode messageBody(String status) {
		assert status != null;
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		if (hasPart()) {
			body.put("part_name", datePart.getName());
			body.put("part_status", status);	
		}
		else {
			body.put("status", status);	
		}
		return body;
	}
	
	void putRunStatus(ObjectNode body) {
		if (logger.isDebugEnabled())
			logger.debug(Log.REQUEST, String.format(
				"putRunStatus %s %s", runKey, body.toString()));
		JsonRequest request = new JsonRequest(session, putRunStatusURI, HttpMethod.PUT, body);		
		ObjectNode response;
		try {
			response = request.execute();
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, String.format(
				"putRunStatus %s %s", runKey, response.toString()));		
	}

	
}
