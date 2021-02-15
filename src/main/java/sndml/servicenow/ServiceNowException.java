package sndml.servicenow;

import java.io.IOException;
import java.net.URI;

@SuppressWarnings("serial")
public class ServiceNowException extends IOException {

	public ServiceNowException(URI uri) {
		super(uri.toString());
	}
	
	public ServiceNowException(URI uri, String requestText) {
		super(Log.joinLines(uri.toString(), requestText));
	}
	
	public ServiceNowException(String message) {
		super(message);
	}
	
	public ServiceNowException(ServiceNowRequest request) {
		super(request.dump());
	}
	
}
