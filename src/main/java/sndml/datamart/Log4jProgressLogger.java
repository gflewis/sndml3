package sndml.datamart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class Log4jProgressLogger extends ProgressLogger {
	
	protected final Logger logger;
	protected final Action action;
	private Integer expected; // used for debugging
	
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
	
	public Action getAction() {
		return action;
	}
	
	private String getOperation() {
		if (action == Action.PRUNE) return "Deleted";
		if (reader instanceof Synchronizer) return "Synchronized";
		return "Processed";
	}
	
	@Override
	public void logPrepare() {
		logger.debug(Log.INIT, "Preparing");
	}
	
	@Override
	public void logStart(Integer expected) {
		this.expected = expected; // used for debugging only
		//TODO: Cleanup
//		ReaderMetrics readerMetrics = reader.getReaderMetrics();
		if (hasPart()) {
			Integer parentExpected = reader.getParent().getExpected();
//			ReaderMetrics parentReaderMetrics = reader.getParent().getReaderMetrics();
			logger.info(Log.INIT, String.format(
				"Starting %s (%d / %d rows)", datePart,	expected, parentExpected));			
		}
		else {
			logger.info(Log.INIT, String.format(
				"Starting (%d rows)", expected));
		}		
}
	@Override
	public void logProgress() {
		assert reader != null;
//		ReaderMetrics readerMetrics = reader.getReaderMetrics();
		Metrics metrics = reader.getMetrics();
		if (hasPart()) {
//			ReaderMetrics parentReaderMetrics = reader.getParent().getReaderMetrics();
			Metrics parentMetrics = metrics.getParent();
			logger.info(Log.PROCESS, String.format(
				"%s %s (%s)", getOperation(), getProgress(metrics), getProgress(parentMetrics)));
//				readerMetrics.getProgress(), 
//				parentReaderMetrics.getProgress())); 					
		}
		else {
			logger.info(Log.PROCESS, String.format(
				"%s %s", getOperation(), getProgress(metrics)));
//				readerMetrics.getProgress()));			
		}
	}

	@Override
	public void logComplete(Metrics writerMetrics) {
		if (logger.isDebugEnabled()) 
			logger.debug(Log.FINISH, String.format(
				"expected=%d %s", expected, writerMetrics.toString()));
		int processed = writerMetrics.getProcessed();
		assert processed == this.expected;
		if (hasPart()) {
			logger.info(Log.FINISH, String.format(
					"Completed %s (%d rows)", datePart, processed));					
		}
		else {
			logger.info(Log.FINISH, String.format(
				"Completed (%d rows)", processed));					
		}
	}

	private static String getProgress(Metrics metrics) {
		if (metrics.hasExpected())
			return String.format("%d / %d", metrics.getInput(), metrics.getExpected());
		else
			return String.format("%d", metrics.getInput());
	}

}
