package servicenow.datamart;

import servicenow.api.ServiceNowException;

// This class is not used
@Deprecated
@SuppressWarnings("serial")
public class LoaderException extends ServiceNowException {

	public LoaderException(String message) {
		super(message);
	}	

}
