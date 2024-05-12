package sndml.servicenow;

import java.net.URI;
import org.slf4j.Logger;

import sndml.util.Log;

import org.apache.http.StatusLine;
import org.apache.http.impl.client.CloseableHttpClient;

public abstract class ServiceNowRequest {

	final CloseableHttpClient client;
	final URI uri;
	final HttpMethod method;
	protected StatusLine statusLine;
	protected int statusCode;
	protected String responseContentType;
	protected String requestText;
	protected String responseText;
		
	ServiceNowRequest(CloseableHttpClient client, URI uri, HttpMethod method) {
		this.client = client;
		this.uri = uri;
		this.method = method;
	}

	public URI getURI() {
		return uri;
	}
	
	public StatusLine getStatusLine() {
		return statusLine;
	}
	
	public int getStatusCode() {
		return statusLine.getStatusCode();
	}
	
	public String dumpRequestText() {
		return requestText;
	}
	
	public String dumpResponseText() {
		return responseText;
	}
	
	public String dump() {
		StringBuilder text = new StringBuilder();
		text.append(method.toString());
		text.append(" ");
		text.append(uri.toString());
		if (statusLine != null) {
			text.append("\n");
			text.append(statusLine);
		}
		if (requestText != null && requestText.length() > 0) {
			text.append("\nREQUEST:\n");
			text.append(dumpRequestText());
		}
		if (responseText != null && responseText.length() > 0) {
			text.append("\nRESPONSE:\n");
			text.append(dumpResponseText());
		}
		return text.toString();
	}
	
	public void logResponseError(Logger logger) {
		logger.error(Log.RESPONSE, dump());
	}
	
}
