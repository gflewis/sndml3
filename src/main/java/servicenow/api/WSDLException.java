package servicenow.api;

import java.io.IOException;

@SuppressWarnings("serial")
class WSDLException extends IOException {

	public WSDLException(String message) {
		super(message);
	}
	
	public WSDLException(Throwable cause) {
		super(cause);
	}

}
