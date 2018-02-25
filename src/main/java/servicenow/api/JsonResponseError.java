package servicenow.api;

public class JsonResponseError extends ServiceNowError {

	private static final long serialVersionUID = 1L;

	public JsonResponseError(String message) {
		super(message);
	}

}
