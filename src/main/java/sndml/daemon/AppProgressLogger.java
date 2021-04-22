package sndml.daemon;

import java.io.IOException;
import java.net.URI;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.ConnectionProfile;
import sndml.datamart.DatePart;
import sndml.datamart.ResourceException;
import sndml.servicenow.*;

public class AppProgressLogger extends ProgressLogger {

	final ConnectionProfile profile;
	final Session session;
	final URI putRunStatusURI;
	final String number;
	final RecordKey runKey;
	final Logger logger = LoggerFactory.getLogger(this.getClass());	

	AppProgressLogger(
			ConnectionProfile profile, 
			Session session,
			Metrics metrics,
			String number, 
			RecordKey runKey) {
		this(profile, session, metrics, number, runKey, null);
	}
	
	AppProgressLogger(
			ConnectionProfile profile, 
			Session session,
			Metrics metrics,
			String number, 
			RecordKey runKey,
			DatePart part) {
		super(metrics, part);
		assert runKey != null;
		this.profile = profile;
		this.session = session;
		this.number = number;
		this.runKey = runKey;
		this.putRunStatusURI = profile.getAPI("putrunstatus");
	}

	@Override
	public ProgressLogger newPartLogger(Metrics newMetrics, DatePart newPart) {
		// logger.info(Log.INIT, "newPartLogger");
		return new AppProgressLogger(
			this.profile, this.session, newMetrics, this.number, this.runKey, newPart);
	}
	
	@Override
	public void logPrepare() {
		putRunStatus(messageBody("prepare"));
	}

	@Override
	public void logStart() {
		int expected = metrics.getExpected();
		logger.info(Log.INIT, String.format("logStart %d", expected));
		ObjectNode body = messageBody("running");
		String fieldname = hasPart() ? "part_expected" : "expected";
		body.put(fieldname, expected);
		putRunStatus(body);
	}
		
	@Override
	public void logProgress() {
		logger.debug(Log.PROCESS, "logProgress");
		assert metrics != null;
		ObjectNode body = messageBody("running");
		appendMetrics(body, metrics);
		putRunStatus(body);
	}

	@Override
	public void logComplete() {
		logger.info(Log.FINISH, "logComplete");
		ObjectNode body = messageBody("complete");
		if (metrics.hasParent()) {
			Metrics parentMetrics = metrics.getParent();
			body.put("part_elapsed", String.format("%.1f", parentMetrics.getElapsedSec()));			
		}
		else {
			body.put("elapsed", String.format("%.1f", metrics.getElapsedSec()));			
		}
		appendMetrics(body, metrics);
		putRunStatus(body);
	}

	/**
	 * Create a new message body
	 */
	private ObjectNode messageBody(String status) {
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
	
	/**
	 * Append metrics to the message body
	 */
	private void appendMetrics(ObjectNode body, Metrics metrics) {
		if (hasPart()) {
			assert metrics.hasParent();
			Metrics parentMetrics = metrics.getParent();
			body.put("expected", parentMetrics.getExpected());
			body.put("inserted", parentMetrics.getInserted());
			body.put("updated",  parentMetrics.getUpdated());
			body.put("deleted",  parentMetrics.getDeleted());
			body.put("skipped",  parentMetrics.getSkipped());
			body.put("part_expected", metrics.getExpected());
			body.put("part_inserted", metrics.getInserted());
			body.put("part_updated",  metrics.getUpdated());
			body.put("part_deleted",  metrics.getDeleted());		
			body.put("part_skipped",  metrics.getSkipped());
		}
		else {
			body.put("expected", metrics.getExpected());
			body.put("inserted", metrics.getInserted());
			body.put("updated",  metrics.getUpdated());
			body.put("deleted",  metrics.getDeleted());
			body.put("skipped",  metrics.getSkipped());
		}		
	}
	
	void putRunStatus(ObjectNode body) {
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
