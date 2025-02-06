package sndml.agent;

import sndml.loader.DatePart;
import sndml.servicenow.RecordKey;
import sndml.util.Metrics;

public class AppStatusPayload {

	final Thread publisher;
	final RecordKey runKey;
	final String number;
	final AppJobStatus status;	
	final DatePart datePart;
	final Metrics metrics;
	
	public AppStatusPayload(
			Thread publisher,
			RecordKey runKey,
			String number,
			AppJobStatus status,			
			DatePart datePart,
			Metrics metrics) 
	{
		this.publisher = publisher;
		this.runKey = runKey;
		this.number = number;
		this.status = status;
		this.datePart = datePart;
		this.metrics = metrics;
	}

}
