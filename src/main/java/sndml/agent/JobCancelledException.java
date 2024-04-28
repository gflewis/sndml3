package sndml.agent;

import sndml.servicenow.RecordKey;

@SuppressWarnings("serial")
public class JobCancelledException extends Exception {
	
	public JobCancelledException(RecordKey runkey) {
		super();
	}

}
