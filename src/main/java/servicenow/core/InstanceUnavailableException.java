package servicenow.core;

import java.net.URI;

public class InstanceUnavailableException extends ServiceNowException {

	private static final long serialVersionUID = 1L;

	public InstanceUnavailableException(URI uri, String responseText) {
		super("Unavailable: " + uri.toString() + "\n" + responseText);
	}
	
}
