package sndml.servicenow;

/*
 * A serious problem that probably indicates a bug in the implementation.
 */
public class ServiceNowError extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ServiceNowError(Throwable e) {
		super(e);
	}
	
	public ServiceNowError(String message) {
		super(message);
	}
}
