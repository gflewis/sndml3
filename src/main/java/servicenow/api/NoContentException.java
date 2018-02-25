package servicenow.api;

import java.net.URI;

public class NoContentException extends ServiceNowException {

	private static final long serialVersionUID = 1L;

	public NoContentException(URI uri) {
		super(uri);
	}

	public NoContentException(URI uri, String requestText) {
		super(uri, requestText);
	}
	
}
