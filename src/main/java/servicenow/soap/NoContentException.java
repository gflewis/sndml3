package servicenow.soap;

import java.net.URI;

public class NoContentException extends SoapResponseException {

	private static final long serialVersionUID = 1L;

	public NoContentException(URI uri) {
		super(uri);
	}


}
