package sndml.datamart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.DatePart;
import sndml.servicenow.Log;
import sndml.servicenow.ProgressLogger;
import sndml.servicenow.ReaderMetrics;
import sndml.servicenow.TableReader;

public class Log4jProgressLogger extends ProgressLogger {
	
	protected final Logger logger;
	protected final Action action;
	
	public Log4jProgressLogger(TableReader reader, Action action)	{
		this(reader, action, null);
	}
	
	public Log4jProgressLogger(TableReader reader, Action action, DatePart part) {
		super(reader, part);
		this.action = action;
		this.logger = LoggerFactory.getLogger(reader.getClass());
	}
	
	public Log4jProgressLogger newPartLogger(TableReader newReader, DatePart newPart) {
		return new Log4jProgressLogger(newReader, action, newPart);
	}
	
	Action getAction() {
		return this.action;
	}
	
	String getOperation() {
		if (action == Action.PRUNE) return "Deleted";
		if (reader instanceof Synchronizer) return "Synced";
		return "Processed";
	}
	
	@Override
	public void logPrepare() {
		// TODO Auto-generated method stub		
	}
	
	@Override
	public void logStart(Integer expected) {
		ReaderMetrics readerMetrics = reader.getReaderMetrics();
		if (hasPart()) {
			ReaderMetrics parentReaderMetrics = reader.getParent().getReaderMetrics();
			logger.info(Log.INIT, String.format(
				"Begin (%d / %d rows)", 
				readerMetrics.getExpected(), parentReaderMetrics.getExpected()));			
		}
		else {
			logger.info(Log.INIT, String.format(
				"Begin (%d rows)",  
				readerMetrics.getExpected()));
		}		
}
	
	public void logProgress() {
		assert reader != null;
		ReaderMetrics readerMetrics = reader.getReaderMetrics();
		if (hasPart()) {
			ReaderMetrics parentReaderMetrics = reader.getParent().getReaderMetrics();
			logger.info(Log.PROCESS, String.format(
				"%s %s (%s)", getOperation(),
				readerMetrics.getProgress(), 
				parentReaderMetrics.getProgress())); 					
		}
		else {
			logger.info(Log.PROCESS, String.format(
				"%s %s", getOperation(), 
				readerMetrics.getProgress()));			
		}
	}

	@Override
	public void logFinish() {
		int processed = reader.getWriterMetrics().getProcessed();		
		logger.info(Log.FINISH, String.format(
				"Completed (%d rows)", processed));
		
	}


}
