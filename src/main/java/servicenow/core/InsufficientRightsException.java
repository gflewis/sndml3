package servicenow.core;

import java.net.URI;

@SuppressWarnings("serial")
public class InsufficientRightsException extends ServiceNowException {

	public InsufficientRightsException(URI uri) {
		super(uri);
	}
	
	public InsufficientRightsException(URI uri, String requestText) {
		super(uri, requestText);
	}
	
	public InsufficientRightsException(String tablename, String method) {
		super("table=" + tablename + " method=" + method);
	}
	
	public InsufficientRightsException(String message) {
		super(message);
	}
}
