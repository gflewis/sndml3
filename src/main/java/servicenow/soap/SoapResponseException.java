package servicenow.soap;

import java.io.IOException;
import java.net.URI;

import servicenow.core.Table;

/**
 * Exception thrown when there is an undetermined problem with a SOAP response.
 */
public class SoapResponseException extends IOException {

	private static final long serialVersionUID = 1L;

	SoapResponseException(URI uri, Exception cause) {
		super(uri.toString(), cause);
	}
	
	SoapResponseException(URI uri) {
		super(uri.toString());
	}
	
	SoapResponseException(Table table, String message) {
		super("table=" + table.getName() + " " + message);
	}
	
	SoapResponseException(Table table, Exception cause, String response) {
		super(response + "\ntable=" + table.getName(), cause);
	}

}
