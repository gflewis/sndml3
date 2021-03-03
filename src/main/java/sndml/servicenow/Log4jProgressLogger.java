package sndml.servicenow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4jProgressLogger extends ProgressLogger {
	
	protected final Logger logger;
	
	public Log4jProgressLogger(TableReader reader)	{
		this.logger = LoggerFactory.getLogger(reader.getClass());
	}
	
	public Log4jProgressLogger(Logger logger) {
		this.logger = logger;
	}
	
	@Override
	public void logStart(TableReader reader, String operation) {
		ReaderMetrics readerMetrics = reader.getReaderMetrics();
		if (reader.hasParent()) {
			ReaderMetrics parentReaderMetrics = reader.getParent().getReaderMetrics();
			logger.info(Log.INIT, String.format(
				"begin %s (%d / %d rows)", 
				operation, readerMetrics.getExpected(), parentReaderMetrics.getExpected()));			
		}
		else {
			logger.info(Log.INIT, String.format(
				"begin %s (%d rows)", 
				operation, readerMetrics.getExpected()));
		}		
}
	
	public void logProgress(TableReader reader) {
		assert reader != null;
		ReaderMetrics readerMetrics = reader.getReaderMetrics();
		if (reader.hasParent()) {
			ReaderMetrics parentReaderMetrics = reader.getParent().getReaderMetrics();
			logger.info(Log.PROCESS, String.format(
				"%s %s (%s)",
				operation, readerMetrics.getProgress(), 
				parentReaderMetrics.getProgress())); 					
		}
		else {
			logger.info(Log.PROCESS, String.format(
				"%s %s", operation, readerMetrics.getProgress()));			
		}
	}

	@Override
	public void logFinish(TableReader reader) {
		int processed = reader.getWriterMetrics().getProcessed();		
		logger.info(Log.FINISH, String.format("end %s (%d rows)", 
				operation, processed));
		
	}

}
