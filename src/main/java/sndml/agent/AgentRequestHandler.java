package sndml.agent;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

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
	private final int DEFAULT_THREAD_COUNT = 3;

	static final ObjectMapper mapper = new ObjectMapper();
	
	public AgentRequestHandler(ConnectionProfile profile) {
		this.profile = profile;
		int threadCount = profile.agent.getInt("threads", DEFAULT_THREAD_COUNT);
		this.workerPool = new WorkerPool(threadCount);
	}
	
	@Override
	public void handle(HttpExchange exchange) throws IOException {
		try {
			URI uri = exchange.getRequestURI();
			logger.info(Log.REQUEST, "Path: " + uri.getPath());
			String[] parts = uri.getPath().split("/");
			if (parts.length < 2) throw new AgentURLException(uri);
			String cmd = parts.length > 1 ? parts[1] : null;
			String arg = parts.length > 2 ? parts[2] : null;
			logger.debug(Log.REQUEST, String.format("len=%d %s %s", parts.length, cmd, arg));			
			if ("startjobrun".equals(cmd)) {
				doJobRunStart(uri, cmd, arg);
			}
			else {
				throw new AgentURLException(uri);				
			}
			// Response length is zero
			exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
			// TODO Is this required if there is no response?
			OutputStream stream = exchange.getResponseBody(); 
			stream.close();
			exchange.close();			
		}
		catch (AgentHandlerException e) {
			logger.error(Log.ERROR, String.format( 
				"Caught %s: %s (status=%d)", 
				e.getClass().getName(), e.getMessage(), e.getReturnCode()));
			exchange.sendResponseHeaders(e.getReturnCode(), 0);
			exchange.close();
		}
		catch (Exception e) {
			// If an unexpected error occurs then shut down the server
			// What could it possibly be?
			logger.error(Log.ERROR, String.format( 
					"Caught %s: %s", 
					e.getClass().getName(), e.getMessage()));
			exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0); // 500
			exchange.close();
			logger.error(Log.FINISH, "Halting the server due to unexpected error");
			Runtime.getRuntime().halt(-1);
		}
	}
	
	// TODO: Why is this using SingleJobRunner and not AppJobRunner?
	void doJobRunStart(URI uri, String cmd, String arg) throws AgentHandlerException {
		if (arg == null) throw new AgentURLException(uri);
		RecordKey sys_id = new RecordKey(arg);
		logger.info(Log.REQUEST, "creating jobrunner");
		try {
			SingleJobRunner jobrunner = new SingleJobRunner(profile, sys_id);
			AppStatusLogger statusLogger = new AppStatusLogger(jobrunner.getAppSession());
			logger.info(Log.REQUEST, "created jobrunner");
			statusLogger.setStatus(sys_id, AppJobStatus.PREPARE);
			workerPool.submit(jobrunner);
		}
		catch (NoContentException | NoSuchRecordException | IllegalStateException e) {
			throw new AgentHandlerException(e, HttpURLConnection.HTTP_NOT_FOUND); // 404
		}
		catch (ResourceException | JobCancelledException e) {
			throw new AgentHandlerException(e, HttpURLConnection.HTTP_UNAVAILABLE); // 503
		} catch (IOException e) {
			throw new AgentHandlerException(e, HttpURLConnection.HTTP_INTERNAL_ERROR); // 500
		}		
	}

}
