package servicenow.core;

import java.io.IOException;
import java.net.URI;

public class ServiceNowException extends IOException {

	private static final long serialVersionUID = 1L;

	public ServiceNowException(URI uri) {
		super(uri.toString());
	}
	
	public ServiceNowException(URI uri, Parameters params, String requestText) {
		super(uri.toString());
	}
	
	public ServiceNowException(String message) {
		super(message);
	}

}
