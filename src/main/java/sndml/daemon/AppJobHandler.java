package sndml.daemon;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.*;

import sndml.datamart.*;
import sndml.servicenow.Log;

public class AppJobHandler implements HttpHandler {

	private final ConnectionProfile profile;
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final ConfigFactory configFactory = new ConfigFactory();
	private final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;

	static final ObjectMapper mapper = new ObjectMapper();
	
	public AppJobHandler(ConnectionProfile profile) {
		this.profile = profile;
	}
	
	@Override
	public void handle(HttpExchange exchange) throws IOException {
		String requestText = new String(exchange.getRequestBody().readAllBytes());
		logger.info(Log.REQUEST, requestText);
		logger.info(Log.REQUEST, "creating jobConfig");
		JobConfig jobConfig;
		try {
			ObjectNode requestObj = (ObjectNode) mapper.readTree(requestText);
			jobConfig = configFactory.jobConfig(profile, requestObj);
			logger.info(Log.REQUEST, jobConfig.toString());
		}
		catch (Exception e) {
			logger.error(Log.ERROR, "Request parse error: " + e.getMessage(), e);			
		}
		
		ObjectNode responseObj = nodeFactory.objectNode();
		responseObj.put("status", "okay");
		String responseText = responseObj.toString();
		
		logger.info(Log.RESPONSE, responseText);
		byte[] responseBytes = responseText.getBytes();
		exchange.sendResponseHeaders(200, responseBytes.length);
		OutputStream stream = exchange.getResponseBody();
		stream.write(responseBytes);
		stream.close();
		
	}

}
