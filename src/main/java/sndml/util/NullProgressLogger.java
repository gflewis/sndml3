package sndml.util;

/**
 * {@link ProgressLogger} that discards metrics.
 */
public class NullProgressLogger extends ProgressLogger {

	public NullProgressLogger() {
		super(null, null);
	}
	
	public NullProgressLogger(Metrics metrics, DatePartition datePart) {
		super(metrics, datePart);
	}
		
	@Override
	public NullProgressLogger newPartLogger(Metrics newMetrics, DatePartition newPart) {
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
