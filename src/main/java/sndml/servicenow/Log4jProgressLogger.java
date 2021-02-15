package sndml.servicenow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4jProgressLogger extends ProgressLogger {
	
	protected final Logger logger;

	public Log4jProgressLogger(@SuppressWarnings("rawtypes") Class clazz) {
		super();
		this.logger = LoggerFactory.getLogger(clazz);
	}
	
	public void logProgress(ReaderMetrics readerMetrics, WriterMetrics writerMetrics) {
		assert this.logger != null;
		assert readerMetrics != null;
		if (readerMetrics.getParent() == null) 
			logger.info(Log.PROCESS, String.format("%s %s", 
				operation, readerMetrics.getProgress()));
		else
			logger.info(Log.PROCESS, String.format("%s %s (%s)", 
				operation, readerMetrics.getProgress(), readerMetrics.getParent().getProgress())); 		
	}

}
