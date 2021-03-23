package sndml.servicenow;

import java.io.IOException;
import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonRequest extends ServiceNowRequest {

	static final ObjectMapper mapper = new ObjectMapper();
	final ObjectNode requestObj;
	ObjectNode responseObj;
	boolean executed = false;
	
	final private Logger logger = LoggerFactory.getLogger(this.getClass());

	public JsonRequest(Session session, URI uri) {
		this(session, uri, HttpMethod.GET, null);
	}
	
	public JsonRequest(Session session, URI uri, HttpMethod method, ObjectNode body) {
		super(session.getClient(), uri, method);
		this.requestObj = body;
	}

	public ObjectNode execute() throws IOException {
		assert executed == false;
		executeRequest();		
		if (responseText == null) return null;
		responseObj = (ObjectNode) mapper.readTree(responseText);
		if (responseObj.has("error")) {
			logger.warn(Log.RESPONSE, method.toString() + " " + uri.toString());
			logger.warn(Log.RESPONSE, responseText);
		}
		return responseObj;
	}
	
	private void executeRequest() throws IOException {
		assert client != null;
		assert uri != null;
		assert method != null;
		HttpUriRequest request;
		HttpEntity requestEntity = null;
		logger.debug(Log.REQUEST, method.name() + " " + uri.toURL());
		if (requestObj != null) {
			requestText = requestObj.toString();
			if (logger.isTraceEnabled()) logger.trace(Log.REQUEST, requestText);
			requestEntity = new StringEntity(requestText, ContentType.APPLICATION_JSON);
		}
		switch (method) {
		case DELETE:
			assert requestText == null;
			HttpDelete httpDelete = new HttpDelete(uri);
			request = httpDelete;
			break;
		case GET:
			assert requestText == null;
			HttpGet httpGet = new HttpGet(uri);
			request = httpGet;
			break;
		case PATCH:
			assert requestText != null;
			HttpPatch httpPatch = new HttpPatch(uri);
			httpPatch.setEntity(requestEntity);
			httpPatch.setHeader("Content-Type", "application/json");
			request = httpPatch;
			break;
		case POST:
			assert requestText != null;
			HttpPost httpPost = new HttpPost(uri);
			httpPost.setEntity(requestEntity);
			httpPost.setHeader("Content-Type", "application/json");
			request = httpPost;
			break;
		case PUT:
			assert requestText != null;
			HttpPut httpPut = new HttpPut(uri);
			httpPut.setEntity(requestEntity);
			httpPut.setHeader("Content-Type", "application/json");
			request = httpPut;
			break;
		default:
			throw new AssertionError();
		}
		request.setHeader("Accept", "application/json");
		CloseableHttpResponse response = client.execute(request);		
		statusLine = response.getStatusLine();		
		statusCode = statusLine.getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		responseContentType = null;
		if (responseEntity == null) {
			responseText = null;
		}
		else {
			Header contentTypeHeader = responseEntity.getContentType();
			if (contentTypeHeader != null) responseContentType = contentTypeHeader.getValue();
			responseText = EntityUtils.toString(responseEntity);			
		}
		response.close();
		int responseLen = responseText == null ? 0 : responseText.length();
		logger.debug(Log.RESPONSE,
				String.format("status=\"%s\" contentType=%s len=%d", 
					statusLine, responseContentType, responseLen));
		if (statusCode == 204) {
			// Success - No Content
			executed = true;
			return;
		}
		if (logger.isTraceEnabled())
			logger.trace(Log.RESPONSE, responseText);
		if (statusCode == 401 || statusCode == 403) {
			logger.error(Log.RESPONSE, this.dump());
			throw new InsufficientRightsException(this);
		}
		if (responseText == null || responseContentType == null) {
			// should have gotten an HTTP 204 for No Content
			// this.logResponseError(logger);
			throw new NoContentException(this);
		}		
		if ("text/html".equals(responseContentType)) {
			throw new InstanceUnavailableException(this);			
		}
		if (statusCode == 400) {
			this.logResponseError(logger);
			throw new NoContentException(this);
		}
		executed = true;
	}
		
	protected String errorMessageLowerCase() {
		assert responseObj != null;
		if (!responseObj.has("error")) return null;
		ObjectNode error = (ObjectNode) responseObj.get("error");
		if (!error.has("message")) return null;
		return error.get("message").asText().toLowerCase();
	}
		
	protected void checkForInsufficientRights() throws IOException {
		String err = errorMessageLowerCase();
		if (err == null) return;
		if (err.contains("not authorized") ||
				err.contains("insufficient rights") ||
				err.contains("no permission")) {
			this.logResponseError(logger);
			throw new InsufficientRightsException(this);			
		}
	}
	
	protected void checkForNoSuchRecord() throws IOException {
		if (recordNotFound()) {
			this.logResponseError(logger);
			throw new NoSuchRecordException(this);					
		}
	}
	
	protected boolean recordNotFound() {
		String err = errorMessageLowerCase();
		if (err == null) return false;
		if (err.equals("no record found")) return true;
		return false;
	}
}
