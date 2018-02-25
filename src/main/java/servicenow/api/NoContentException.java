package servicenow.api;

import java.net.URI;

public class NoContentException extends ServiceNowException {

	private static final long serialVersionUID = 1L;

	@Deprecated
	public NoContentException(URI uri) {
		super(uri);
	}

	@Deprecated
	public NoContentException(URI uri, String requestText) {
		super(uri, requestText);
	}

	public NoContentException(ServiceNowRequest request) {
		super(request);
	}
}
