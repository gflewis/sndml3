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
	
	AppRunLogger(ConnectionProfile profile, Session session, String number, Key runKey) {
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
		ObjectNode response = request.getObject();
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
			request.getObject();
		} catch (IOException e1) {
			logger.error(Log.FINISH, "Unable to log Error: " + e.getMessage());
		}		
	}
	
	@Override
	public void logProgress(TableReader reader) {
		assert runKey != null;
		ReaderMetrics readerMetrics = reader.getReaderMetrics();
		WriterMetrics writerMetrics = reader.getWriterMetrics();
		assert readerMetrics != null;
		assert writerMetrics != null;
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", runKey.toString());		
		body.put("status", "running");
		if (reader.getPartName() != null) {
			assert readerMetrics.hasParent();
			assert writerMetrics.hasParent();
			body.put("part_name", reader.getPartName());
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
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		ObjectNode response;
		try {
			response = request.getObject();
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug(Log.RESPONSE, "logProgress " + runKey + " " + response.toString());
	}

	@Override
	public void logStart(TableReader reader, String operation) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void logFinish(TableReader reader) {
		// TODO Auto-generated method stub
		
	}
	

}
