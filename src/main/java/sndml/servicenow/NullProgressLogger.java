package sndml.servicenow;

/**
 * {@link ProgressLogger} that discards metrics.
 *
 */
public class NullProgressLogger extends ProgressLogger {

	public NullProgressLogger() {
		super();
	}

	@Override
	public void logProgress(ReaderMetrics readerMetrics, WriterMetrics writerMetrics) {
		return;
	}
	
}
