package sndml.servicenow;

import java.net.URI;
import org.slf4j.Logger;
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
		if (requestText != null) {
			text.append("\nREQUEST:\n");
			text.append(dumpRequestText());
		}
		if (responseText != null) {
			text.append("\nRESPONSE:\n");
			text.append(dumpResponseText());
		}
		return text.toString();
	}
	
	public void logResponseError(Logger logger) {
		logger.error(Log.RESPONSE, dump());
	}
	
}
