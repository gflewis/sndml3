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
			TableReader reader, 
			String number, 
			Key runKey) {
		this(profile, session, reader, number, runKey, null);
	}
	
	DaemonProgressLogger(
			ConnectionProfile profile, 
			Session session, 
			TableReader reader, 
			String number, 
			Key runKey,
			DatePart part) {
		super(reader, part);
		assert runKey != null;
		this.profile = profile;
		this.session = session;
		this.number = number;
		this.runKey = runKey;
		String putRunStatusPath = profile.getProperty(
			"loader.api.putrunstatus", 
			"api/x_108443_sndml/putrunstatus");
		this.putRunStatusURI = session.getURI(putRunStatusPath);		
	}

	@Override
	public DaemonProgressLogger newPartLogger(TableReader newReader, DatePart newPart) {
		return new DaemonProgressLogger(profile, session, newReader, number, runKey, newPart); 
	}

	@Override
	public void logPrepare() {
		putRunStatus(messageBody("prepare"));
	}

	@Override
	public void logStart(Integer expected) {	
		ObjectNode body = messageBody("running");
		String fieldname = hasPart() ? "part_expected" : "expected";
		body.put(fieldname,  expected);
		putRunStatus(body);
	}
		
	@Override
	public void logProgress() {
		ReaderMetrics readerMetrics = reader.getReaderMetrics();
		WriterMetrics writerMetrics = reader.getWriterMetrics();
		assert readerMetrics != null;
		assert writerMetrics != null;
		ObjectNode body = messageBody("running");
		if (hasPart()) {
			assert readerMetrics.hasParent();
			assert writerMetrics.hasParent();
			body.put("expected", readerMetrics.getParent().getExpected());
			body.put("inserted",  writerMetrics.getParent().getInserted());
			body.put("updated",  writerMetrics.getParent().getUpdated());
			body.put("deleted",  writerMetrics.getParent().getDeleted());			
			body.put("part_expected", readerMetrics.getExpected());
			body.put("part_inserted",  writerMetrics.getInserted());
			body.put("part_updated",  writerMetrics.getUpdated());
			body.put("part_deleted",  writerMetrics.getDeleted());						
		}
		else {
			body.put("expected", readerMetrics.getExpected());
			body.put("inserted",  writerMetrics.getInserted());
			body.put("updated",  writerMetrics.getUpdated());
			body.put("deleted",  writerMetrics.getDeleted());			
		}
		putRunStatus(body);
	}

	@Override
	public void logComplete() {
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
			response = request.getObject();
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, String.format(
				"putRunStatus %s %s", runKey, response.toString()));		
	}
	
}
