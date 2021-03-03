package sndml.servicenow;

/**
 * {@link ProgressLogger} that discards metrics.
 *
 */
public class NullProgressLogger extends ProgressLogger {
	
	@Override
	public void logStart(TableReader reader, String operation) {
		return;
	}
	
	@Override
	public void logProgress(TableReader reader) {
		return;		
	}

	@Override
	public void logFinish(TableReader reader) {
		return;
	}

}
