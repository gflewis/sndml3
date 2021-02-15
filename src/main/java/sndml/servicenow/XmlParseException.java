package sndml.servicenow;

import java.net.URI;

class XmlParseException extends SoapResponseException {

	private static final long serialVersionUID = 1L;

	public XmlParseException(URI uri, Exception cause) {
		super(uri, cause);
	}


}
