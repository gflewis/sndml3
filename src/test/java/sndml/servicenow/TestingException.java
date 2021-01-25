package sndml.servicenow;

/**
 * An exception thrown by the unit testing framework.
 */
public class TestingException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;

	public TestingException(Throwable cause) {
		super(cause);
	}
	
	public TestingException(String message) {
		super(message);
	}
	
	public TestingException(String message, Throwable cause) {
		super(message, cause);
	}

}
