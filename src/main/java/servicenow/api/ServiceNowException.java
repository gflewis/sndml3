package servicenow.api;

import java.io.IOException;
import java.net.URI;

public class ServiceNowException extends IOException {

	private static final long serialVersionUID = 1L;

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
		super(request.toString());
	}
	
}
