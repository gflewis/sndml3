package sndml.servicenow;

import java.io.IOException;
import java.net.URI;

import org.jdom2.Element;

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
	
	SoapResponseException(Table table, String method, String message, Element responseElement) {
		super(Log.joinLines(
				String.format("table=%s method=%s %s", table.getName(), method, message), 
				XmlFormatter.format(responseElement)));
	}
	
	SoapResponseException(String tablename, String message) {
		super("table=" + tablename + " " + message);
	}
	
	SoapResponseException(Table table, Exception cause, String response) {
		super(response + "\ntable=" + table.getName(), cause);
	}

}
