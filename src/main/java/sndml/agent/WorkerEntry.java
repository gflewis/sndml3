package sndml.agent;

import java.util.concurrent.Future;

import sndml.servicenow.RecordKey;
import sndml.util.Metrics;

class WorkerEntry {
		
	final AppJobConfig config;
	final AppJobRunner runner;
	final String number;
	final RecordKey key;
	final Future<Metrics> future;
	
	WorkerEntry(AppJobRunner runner, Future<Metrics> future) {
		this.runner = runner;
		this.config = runner.config;
		assert config != null;
		this.number = config.getNumber();
		assert number != null;
		this.key = config.getRunKey();
		this.future = future;
	}

}
