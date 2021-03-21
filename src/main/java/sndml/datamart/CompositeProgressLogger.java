package sndml.datamart;

import sndml.servicenow.*;

public class CompositeProgressLogger extends ProgressLogger {

	public final Log4jProgressLogger textLogger;
	public final DaemonProgressLogger appLogger;

	public CompositeProgressLogger(TableReader reader, Action action, DaemonProgressLogger appLogger) {
		this(reader, action, appLogger, null);
	}
	
	@Deprecated
	public CompositeProgressLogger(TableReader reader, Action action, 
			DaemonProgressLogger appLogger, DatePart part) {
		super(appLogger.getMetrics(), part);
		this.textLogger = new Log4jProgressLogger(reader, action);
		this.appLogger = appLogger; 		
	}

	public CompositeProgressLogger(Log4jProgressLogger textLogger, DaemonProgressLogger appLogger) {
		super(textLogger.getMetrics(), textLogger.getPart());
		this.textLogger = textLogger;
		this.appLogger = appLogger;
	}
	
	@Override
	public CompositeProgressLogger newPartLogger(Metrics newMetrics, DatePart newPart) {
		Log4jProgressLogger newTextLogger =
				(Log4jProgressLogger) textLogger.newPartLogger(newMetrics,  newPart);
		DaemonProgressLogger newAppLogger = 
			(DaemonProgressLogger) appLogger.newPartLogger(newMetrics, newPart);
		return new CompositeProgressLogger(newTextLogger, newAppLogger);
	}
	
	@Override
	public void logPrepare() {
		if (appLogger != null) appLogger.logPrepare();
		if (textLogger != null) textLogger.logPrepare();		
	}
	
	@Override
	public void logStart(Integer expected) {
		if (appLogger != null) appLogger.logStart(expected);
		if (textLogger != null) textLogger.logStart(expected);		
	}
	
	@Override
	public void logProgress() {
		if (appLogger != null) appLogger.logProgress();
		if (textLogger != null) textLogger.logProgress();
	}


	@Override
	public void logComplete() {
		if (appLogger != null) appLogger.logComplete();
		if (textLogger != null) textLogger.logComplete();
		
	}

}
