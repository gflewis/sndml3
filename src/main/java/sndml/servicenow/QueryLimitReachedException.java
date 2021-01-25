package sndml.servicenow;

import java.net.URI;

public class QueryLimitReachedException extends ServiceNowException {

	private static final long serialVersionUID = 1L;

	public QueryLimitReachedException(URI uri) {
		super(uri);
	}
	
}
