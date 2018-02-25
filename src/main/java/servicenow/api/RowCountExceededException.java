package servicenow.api;

public class RowCountExceededException extends ServiceNowException {

	private static final long serialVersionUID = 1L;

	public RowCountExceededException(Table table, String message) {
		super(message);
	}

}
