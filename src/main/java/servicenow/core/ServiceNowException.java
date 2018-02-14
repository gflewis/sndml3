package servicenow.core;

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
	
//	protected static String truncate(String message) {
//		if (message == null) return null;
//		final int limit = 200;
//		if (message.length() < limit) 
//			return message;
//		else
//			return message.substring(0,  limit) + "...";					
//		
//	}
}
