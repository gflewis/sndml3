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
	final DaemonStatusLogger statusLogger;
	final Logger logger = LoggerFactory.getLogger(DaemonProgressLogger.class);	

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
		this.profile = profile;
		this.session = session;
		this.number = number;
		this.runKey = runKey;
		this.statusLogger = new DaemonStatusLogger(profile, session);
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
		putRunStatus(messageBody("running"));
	}
		
	@Override
	public void logProgress() {
		assert reader != null;
		assert runKey != null;
		ReaderMetrics readerMetrics = reader.getReaderMetrics();
		WriterMetrics writerMetrics = reader.getWriterMetrics();
		assert readerMetrics != null;
		assert writerMetrics != null;
		ObjectNode body = messageBody("running");
		if (hasPart()) {
			assert readerMetrics.hasParent();
			assert writerMetrics.hasParent();
			body.put("part_name", datePart.getName());
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
		/*
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		ObjectNode response;
		try {
			response = request.getObject();
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "logProgress " + runKey + " " + response.toString());
		*/
	}

	@Override
	public void logFinish() {
		// TODO Auto-generated method stub		
	}

	public void setStatus(String status) throws IOException {
		statusLogger.setStatus(runKey, status);
	}	
	
	public void logError(Exception e) {
		statusLogger.logError(runKey, e);
	}

	ObjectNode messageBody(String status) {
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", status);
		return body;
	}
	
	void putRunStatus(ObjectNode body) {
		JsonRequest request = new JsonRequest(session, putRunStatusURI, HttpMethod.PUT, body);
		ObjectNode response;
		try {
			response = request.getObject();
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "logProgress " + runKey + " " + response.toString());		
	}
	
}
