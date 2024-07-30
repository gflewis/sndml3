package sndml.servicenow;

import java.io.IOException;
import java.net.URI;
import org.apache.http.StatusLine;

import sndml.util.Log;

@SuppressWarnings("serial")
public class ServiceNowException extends IOException {
	
	private ServiceNowRequest request;

	public ServiceNowException(URI uri) {
		super(uri.toString());
	}
	
	public ServiceNowException(RecordKey key) {
		super("sys_id=" + key.toString());
	}
	
	public ServiceNowException(URI uri, String requestText) {
		super(Log.joinLines(uri.toString(), requestText));
	}
	
	public ServiceNowException(String message) {
		super(message);
	}
	
	public ServiceNowException(ServiceNowRequest request) {			
		super(request.dump());
		this.request = request;
	}
	
	public ServiceNowException(ServiceNowRequest request, String message) {
		super(message);
		this.request = request;
	}
	
	public ServiceNowException(ServiceNowRequest request, StatusLine status, String message) {
		super(message);
		this.request = request;
	}
	
	public int getStatusCode() {
		assert request != null: "No request in throw";
		return request.getStatusCode();
	}
	
	public URI getURL() {
		return request.getURI();
	}
	
}
