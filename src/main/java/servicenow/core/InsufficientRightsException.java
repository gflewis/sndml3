package servicenow.core;

import java.net.URI;

@SuppressWarnings("serial")
public class InsufficientRightsException extends ServiceNowException {

	public InsufficientRightsException(URI uri, Parameters params, String requestText) {
		super(uri, params, requestText);
	}
	
	public InsufficientRightsException(
			String tablename, String method) {
		super("table=" + tablename + " method=" + method);
	}
}
