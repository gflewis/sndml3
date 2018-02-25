package servicenow.api;

import java.net.URI;

import org.apache.http.StatusLine;
import org.apache.http.impl.client.CloseableHttpClient;

public abstract class ServiceNowRequest {

	final CloseableHttpClient client;
	final URI uri;
	final HttpMethod method;
	protected StatusLine statusLine;
	protected String responseContentType;
	protected String requestText;
	protected String responseText;
		
	ServiceNowRequest(CloseableHttpClient client, URI uri, HttpMethod method) {
		this.client = client;
		this.uri = uri;
		this.method = method;
	}
		
	public String toString() {
		StringBuilder text = new StringBuilder();
		text.append(uri.toString());
		text.append(" ");
		text.append(method.toString());
		if (statusLine != null) {
			text.append("\n");
			text.append(statusLine);
		}
		if (requestText != null) {
			text.append("\nREQUEST:\n");
			text.append(requestText);
		}
		if (responseText != null) {
			text.append("\nRESPONSE:\n");
			text.append(responseText);
		}
		return text.toString();
	}
}
