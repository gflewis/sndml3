package sndml.servicenow;

public class JsonResponseException extends ServiceNowException {

	private static final long serialVersionUID = 1L;
		
	public JsonResponseException(JsonRequest request) {
		super(request);
	}

}
