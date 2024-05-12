package sndml.agent;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;

import sndml.loader.*;
import sndml.servicenow.NoContentException;
import sndml.servicenow.NoSuchRecordException;
import sndml.servicenow.RecordKey;
import sndml.util.Log;
import sndml.util.ResourceException;

public class AgentRequestHandler implements HttpHandler {

	private final ConnectionProfile profile;
	private final WorkerPool workerPool;
	private final Logger logger = LoggerFactory.getLogger(AgentRequestHandler.class);

	static final ObjectMapper mapper = new ObjectMapper();
	
	public AgentRequestHandler(ConnectionProfile profile) {
		this.profile = profile;
		this.workerPool = new WorkerPool(profile);
	}
	
	@Override
	public void handle(HttpExchange exchange) throws IOException {
		int returnCode = HttpURLConnection.HTTP_OK;
//		String responseText = "okay";
		try {
			URI uri = exchange.getRequestURI();
			String path = uri.getPath();
			String[] parts = path.split("/");
			String requestBody = new String(exchange.getRequestBody().readAllBytes());
			logger.info(Log.REQUEST, "Path: " + uri.getPath());
			if (requestBody.length() > 0)
				logger.info(Log.REQUEST, "Body:\n" + requestBody);
			else 
				logger.info(Log.REQUEST, "Body is empty");
			logger.info(Log.REQUEST, String.format("len=%d %s %s", parts.length, parts[0], parts[1]));
			String command = parts[1];
			if ("jobrun".equals(command)) {
				RecordKey sys_id = new RecordKey(parts[2]);
				logger.info(Log.REQUEST, "creating jobrunner");
				try {
					SingleJobRunner jobrunner = new SingleJobRunner(profile, sys_id);
					AppStatusLogger statusLogger = new AppStatusLogger(jobrunner.getAppSession());
					logger.info(Log.REQUEST, "created jobrunner");
					statusLogger.setStatus(sys_id, AppJobStatus.PREPARE);
					workerPool.submit(jobrunner);
				}
				catch (NoContentException | NoSuchRecordException | IllegalStateException e) {
					returnCode = HttpURLConnection.HTTP_NOT_FOUND; // 404
//					responseText = "Not Found";
					logger.error(Log.ERROR, e.getMessage());										
				}
				catch (ResourceException e) {
					returnCode = HttpURLConnection.HTTP_UNAVAILABLE; // 503
//					responseText = e.getMessage();
					logger.error(Log.ERROR, e.getMessage());
				}
				catch (Exception e) {
					logger.error(Log.ERROR, e.getMessage(), e);
					Runtime.getRuntime().halt(-1);
				}
				// workerPool.submit(jobrunner);
			}
			else {
				returnCode = HttpURLConnection.HTTP_BAD_REQUEST; // 400
				
			}
//			byte[] responseBytes = responseText.getBytes();
			exchange.sendResponseHeaders(returnCode, 0);
			OutputStream stream = exchange.getResponseBody();
//			stream.write(responseBytes);
			stream.close();
			exchange.close();			
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			Runtime.getRuntime().halt(-1);
		}
		
	}

}
