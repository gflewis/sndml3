package sndml.servicenow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressLogger {

	public final Logger logger;
	String operation = "Processed";
	
	public ProgressLogger() {
		logger = LoggerFactory.getLogger(this.getClass());
	}
	
	public ProgressLogger(Logger logger) {
		this.logger = logger;
	}
			
	public void logProgress(ReaderMetrics readerMetrics, WriterMetrics writerMetrics) {
		assert logger != null;
		assert readerMetrics != null;
		if (readerMetrics.getParent() == null) 
			logger.info(Log.PROCESS, String.format("%s %s", 
				operation, readerMetrics.getProgress()));
		else
			logger.info(Log.PROCESS, String.format("%s %s (%s)", operation, 
				readerMetrics.getProgress(), readerMetrics.getParent().getProgress())); 
		
	}
}
