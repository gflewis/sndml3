package sndml.agent;

import sndml.servicenow.RecordKey;
import sndml.servicenow.ServiceNowException;

/**
 * This exception is thrown when an attempt to update the ServiceNow scoped app 
 * receives a 410 error indicating that the Job Run has been cancelled.
 */
@SuppressWarnings("serial")
public class JobCancelledException extends ServiceNowException {
	
	public JobCancelledException(RecordKey jobKey) {
		super("Job cancellation detected for " + jobKey.toString());
	}

}
