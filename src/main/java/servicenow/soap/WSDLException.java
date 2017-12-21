package servicenow.soap;

import java.io.IOException;

@SuppressWarnings("serial")
public class WSDLException extends IOException {

	public WSDLException(String message) {
		super(message);
	}
	
	public WSDLException(Throwable cause) {
		super(cause);
	}

}
