package sndml.datamart;

/**
 * Exception thrown while trying to obtain a resource such as a database connection
 * or a ServiceNow session. These exceptions are generally not recoverable
 * and will cause the Server to abend.
 */
@SuppressWarnings("serial")
public class ResourceException extends RuntimeException {

	public ResourceException(String message) {
		super(message);
	}
	
	public ResourceException(Throwable e) {
		super(e);
	}
	
	public ResourceException(String message, Throwable e) {
		super(message, e);
	}

}
