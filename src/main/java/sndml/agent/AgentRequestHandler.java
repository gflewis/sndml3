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

	private final Resources resources;
	private final AppSession appSession; 
	private final WorkerPool workerPool;
	private final String agentKey;
	private final Logger logger = LoggerFactory.getLogger(AgentRequestHandler.class);

	static final ObjectMapper mapper = new ObjectMapper();
	
	public AgentRequestHandler(Resources resources)  throws ResourceException {
		this.resources = resources;
		this.appSession = resources.getAppSession();
		this.agentKey = appSession.getAgentKey().toString();
		this.workerPool = resources.getWorkerPool();
		AgentMain.writePidFile();
	}
	
	@Override
	public void handle(HttpExchange exchange) throws IOException {
		Log.setGlobalContext();
		try {
			URI uri = exchange.getRequestURI();
			logger.info(Log.REQUEST, "Path: " + uri.getPath());
			String[] parts = uri.getPath().split("/");
			if (parts.length < 3) throw new AgentURLException(uri);
			String cmd = parts.length > 1 ? parts[1] : null;
			String arg1 = parts.length > 2 ? parts[2] : null;
			String arg2 = parts.length > 3 ? parts[3] : null;
			if (!RecordKey.isGUID(arg1)) throw new AgentURLException(uri);
			if (!RecordKey.isGUID(arg2)) throw new AgentURLException(uri);
			logger.debug(Log.REQUEST, String.format("len=%d %s %s %s", parts.length, cmd, arg1, arg2));
			if (!agentKey.equals(arg1)) throw new AgentURLException(uri);
			RecordKey runKey = new RecordKey(arg2);
			if ("startjobrun".equals(cmd)) {
				doJobRunStart(uri, runKey);
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
				e.getClass().getName(), e.getMessage(), e.getReturnCode()), e);
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

	void doJobRunStart(URI uri, RecordKey runKey) throws AgentHandlerException {
		logger.info(Log.REQUEST, "creating jobrunner");
		try {
			AppConfigFactory factory = new AppConfigFactory(appSession);
			AppJobConfig jobconfig = factory.appJobConfig(runKey);			
			if (jobconfig.getStatus() != AppJobStatus.READY) {
				logger.error(Log.REQUEST, String.format(
						"%s has invalid state: %s", jobconfig.getName(), jobconfig.getStatus()));
				throw new AgentHandlerException(uri, HttpURLConnection.HTTP_NOT_FOUND);
			}
			Resources workerResources = resources.workerCopy();
			AppJobRunner jobrunner = new AppJobRunner(workerResources, jobconfig);
			AppStatusLogger statusLogger = new AppStatusLogger(appSession);
			logger.info(Log.REQUEST, "created jobrunner");
			statusLogger.setStatus(runKey, AppJobStatus.PREPARE);
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
	
//	void doJobRunCancel(URI uri, String sys_id) {
//		throw new IllegalStateException("Not implemented");
//		RecordKey runKey = new RecordKey(sys_id); 
//			
//		}
//		catch (Exception e) {
//			
//		}
//	}
	

}
