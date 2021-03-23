package sndml.servicenow;

public class InstanceUnavailableException extends ServiceNowException {

	private static final long serialVersionUID = 1L;

//	@Deprecated
//	public InstanceUnavailableException(URI uri, String responseText) {
//		super(uri.toString() + "\n" + responseText);
//	}

	public InstanceUnavailableException(ServiceNowRequest request) {
		super(request);
	}
	
}
