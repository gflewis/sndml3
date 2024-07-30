package sndml.agent;

import sndml.servicenow.RecordKey;
import sndml.servicenow.ServiceNowException;

@SuppressWarnings("serial")
public class JobCancelledException extends ServiceNowException {
	
	public JobCancelledException(RecordKey jobKey) {
		super(jobKey);
	}

}
