package sndml.agent;

import sndml.loader.DatePart;
import sndml.servicenow.RecordKey;
import sndml.util.Metrics;

public class AppStatusPayload {

	public enum Type {
		STATUS,
		PROGRESS,
		HEARTBEAT
	}
	
	final Type type;
	final Thread publisher;
	final RecordKey runKey;
	final String number;
	final AppJobStatus status;	
	final DatePart datePart;
	final Metrics metrics;
	
	public AppStatusPayload(
			Type type,
			Thread publisher,
			RecordKey runKey,
			String number,
			AppJobStatus status,			
			DatePart datePart,
			Metrics metrics) 
	{
		this.type = type;
		this.publisher = publisher;
		this.runKey = runKey;
		this.number = number;
		this.status = status;
		this.datePart = datePart;
		this.metrics = metrics;
	}
	
	public AppStatusPayload newHeartBeat() {
		return new AppStatusPayload(
				Type.HEARTBEAT, null, null, null, null, null, null);
	}
	
	public AppStatusPayload newStatusPayload(
			Thread publisher,
			RecordKey runKey,
			String number,
			AppJobStatus status) {
		return new AppStatusPayload(
			Type.STATUS, publisher, runKey, number, status, null, null);		
	}
	
	public void process() {
		// TODO Implement AppStatusPayload.process()
		throw new IllegalStateException();
	}

}
