package sndml.servicenow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4jProgressLogger extends ProgressLogger {
	
	protected final Logger logger;

	public Log4jProgressLogger(TableReader reader, @SuppressWarnings("rawtypes") Class clazz) {
		super(reader);
		this.logger = LoggerFactory.getLogger(clazz);
	}
	
	public void logProgress() {
		ReaderMetrics readerMetrics = getReaderMetrics();
		if (readerMetrics == null) return;
		if (readerMetrics.getParent() == null) 
			logger.info(Log.PROCESS, String.format("%s %s", 
				operation, readerMetrics.getProgress()));
		else
			logger.info(Log.PROCESS, String.format("%s %s (%s)", 
				operation, readerMetrics.getProgress(), readerMetrics.getParent().getProgress())); 		
	}
	
//	public void logPartProgress(
//			String partName, 
//			ReaderMetrics readerMetrics, 
//			WriterMetrics writerMetrics) {
//		ReaderMetrics parentMetrics = readerMetrics.getParent();
//		assert parentMetrics != null;
//		logger.info(Log.PROCESS, String.format("%s %s (%s)", 
//				operation, readerMetrics.getProgress(), parentMetrics.getProgress())); 						
//	}

}
