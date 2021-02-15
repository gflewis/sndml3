package sndml.servicenow;

import java.net.URI;

@SuppressWarnings("serial")
public class InsufficientRightsException extends ServiceNowException {

	@Deprecated
	public InsufficientRightsException(URI uri) {
		super(uri);
	}
	
	@Deprecated
	public InsufficientRightsException(URI uri, String requestText) {
		super(uri, requestText);
	}
	
	@Deprecated
	public InsufficientRightsException(String tablename, String method) {
		super("table=" + tablename + " method=" + method);
	}
	
	public InsufficientRightsException(String message) {
		super(message);
	}
	
	public InsufficientRightsException(ServiceNowRequest request) {
		super(request);
	}
	
}
