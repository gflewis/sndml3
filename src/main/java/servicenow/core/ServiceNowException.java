package servicenow.core;

import java.io.IOException;
import java.net.URI;

public class ServiceNowException extends IOException {

	private static final long serialVersionUID = 1L;

	public ServiceNowException(URI uri) {
		super(uri.toString());
	}
	
	public ServiceNowException(URI uri, String requestText) {
		super(uri.toString() + "\n" + truncate(requestText));
	}
	
	public ServiceNowException(String message) {
		super(message);
	}

	public static String truncate(String message) {
		final int limit = 200;
		if (message.length() < limit) 
			return message;
		else
			return message.substring(0,  limit) + "...";					
		
	}
}
