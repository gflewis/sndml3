package sndml.datamart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class Log4jProgressLogger extends ProgressLogger {
	
	protected final Logger logger;
	protected final Action action;
	
	public Log4jProgressLogger(TableReader reader, Action action)	{
		this(reader, action, null);
	}
	
	public Log4jProgressLogger(TableReader reader, Action action, DatePart part) {
		this(reader.getClass(), action, null, null);
	}

	@SuppressWarnings("rawtypes")
	public Log4jProgressLogger(Class clazz, Action action, Metrics metrics) {
		this(clazz, action, metrics, null);
	}
	
	@SuppressWarnings("rawtypes")
	public Log4jProgressLogger(Class clazz, Action action, Metrics metrics, DatePart part) {
		super(metrics, part);
		assert action != null;
		assert metrics != null;
		this.action = action;
		this.logger = LoggerFactory.getLogger(clazz);
	}
	
	protected Log4jProgressLogger(Logger logger, Action action, Metrics metrics, DatePart part) {
		super(metrics, part);
		assert action != null;
		assert metrics != null;
		this.logger = logger;
		this.action = action;
	}
		
			
	public Log4jProgressLogger newPartLogger(TableReader newReader, DatePart newPart) {
		return new Log4jProgressLogger(newReader, action, newPart);
	}

	@Override
	public ProgressLogger newPartLogger(Metrics newMetrics, DatePart newPart) {
		return new Log4jProgressLogger(logger, action, newMetrics, newPart);
	}

	
	public Action getAction() {
		return action;
	}
	
	private String getOperation() {
		if (action == Action.PRUNE) return "Deleted";
		if (action == Action.SYNC) return "Synchronized";
		return "Processed";
	}
	
	@Override
	public void logPrepare() {
		logger.debug(Log.INIT, "Preparing");
	}
	
	@Override
	public void logStart() {
		int expected = metrics.getExpected();
		if (hasPart()) {
			assert metrics.hasParent();			
			Integer parentExpected = metrics.getParent().getExpected();
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
		if (hasPart()) {
			Metrics parentMetrics = metrics.getParent();
			logger.info(Log.PROCESS, String.format(
				"%s %s (%s)", getOperation(), getProgress(metrics), getProgress(parentMetrics)));
		}
		else {
			logger.info(Log.PROCESS, String.format(
				"%s %s", getOperation(), getProgress(metrics)));
		}
	}

	@Override
	public void logComplete() {
		int processed = metrics.getProcessed();
		int expected = metrics.getExpected();
		if (processed != expected) {
			logger.warn(Log.FINISH, String.format(
				"Expected %d rows but only processed %d rows", expected, processed));
		}
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
