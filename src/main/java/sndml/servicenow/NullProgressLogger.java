package sndml.servicenow;

import sndml.datamart.DatePart;

/**
 * {@link ProgressLogger} that discards metrics.
 */
public class NullProgressLogger extends ProgressLogger {

	public NullProgressLogger() {
		super(null, null);
	}
	
	public NullProgressLogger(Metrics metrics, DatePart datePart) {
		super(metrics, datePart);
	}
		
	@Override
	public NullProgressLogger newPartLogger(Metrics newMetrics, DatePart newPart) {
		return new NullProgressLogger(null, newPart);
	}
	
	@Override
	public void logPrepare() {	
	}

	@Override
	public void logStart() {
		return;		
	}

	@Override
	public void logProgress() {
		return;		
	}

	@Override
	public void logComplete() {
		return;
	}


}
