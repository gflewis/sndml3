package sndml.servicenow;

@Deprecated
public class RowCountMismatchException extends ServiceNowException {

	private static final long serialVersionUID = 1L;

	public RowCountMismatchException(Table table, int expected, int retrieved) {
		super(String.format("%s expected=%d retrieved=%d", table.getName(), expected, retrieved));
	}

}
