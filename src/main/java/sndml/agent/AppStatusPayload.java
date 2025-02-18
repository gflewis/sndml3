package sndml.agent;

import sndml.util.Partition;
import sndml.util.Metrics;

public class AppStatusPayload {

	public enum Type {
		STATUS,
		PROGRESS,
		HEARTBEAT
	}
	
	final Type type;
	final AppJobRunner runner;
	final AppJobStatus status;	
	final Partition datePart;
	final Metrics metrics;
	
	public AppStatusPayload(
			Type type,
			AppJobRunner runner,
			AppJobStatus status,			
			Partition datePart,
			Metrics metrics) 
	{
		this.type = type;
		this.runner = runner;
		this.status = status;
		this.datePart = datePart;
		this.metrics = metrics;
	}
	
	public AppStatusPayload newHeartBeat() {
		return new AppStatusPayload(
				Type.HEARTBEAT, null, null, null, null);
	}
	
	public AppStatusPayload newStatusPayload(
			AppJobRunner runner,
			AppJobStatus status) {
		return new AppStatusPayload(
			Type.STATUS, runner, status, null, null);		
	}
	
	public AppStatusPayload newProgressStatusPayload(
			AppJobRunner runner,
			AppJobStatus status,
			Metrics metrics) {
		return new AppStatusPayload(Type.PROGRESS, runner, status, null, metrics);
	}
		
	public AppStatusPayload newPartitionProgressPayload(
			AppJobRunner runner,
			AppJobStatus status,
			Partition datePart,
			Metrics metrics) {
		return new AppStatusPayload(Type.PROGRESS, runner, status, datePart, metrics);
	}
		
	public void process() {
		// TODO Implement AppStatusPayload.process()
		throw new IllegalStateException();
	}

}
