package sndml.servicenow;

public class NoContentException extends ServiceNowException {

	private static final long serialVersionUID = 1L;

	public NoContentException(ServiceNowRequest request) {
		super(request);
	}
}
