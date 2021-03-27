package sndml.datamart;

import sndml.daemon.AppProgressLogger;
import sndml.servicenow.*;

public class CompositeProgressLogger extends ProgressLogger {

	public final Log4jProgressLogger textLogger;
	public final AppProgressLogger appLogger;

	public CompositeProgressLogger(TableReader reader, Action action, AppProgressLogger appLogger) {
		this(reader, action, appLogger, null);
	}
	
	@Deprecated
	public CompositeProgressLogger(TableReader reader, Action action, 
			AppProgressLogger appLogger, DatePart part) {
		super(appLogger.getMetrics(), part);
		this.textLogger = new Log4jProgressLogger(reader, action);
		this.appLogger = appLogger; 		
	}

	public CompositeProgressLogger(Log4jProgressLogger textLogger, AppProgressLogger appLogger) {
		super(textLogger.getMetrics(), textLogger.getPart());
		this.textLogger = textLogger;
		this.appLogger = appLogger;
	}
	
	@Override
	public CompositeProgressLogger newPartLogger(Metrics newMetrics, DatePart newPart) {
		Log4jProgressLogger newTextLogger =
				(Log4jProgressLogger) textLogger.newPartLogger(newMetrics,  newPart);
		AppProgressLogger newAppLogger = 
			(AppProgressLogger) appLogger.newPartLogger(newMetrics, newPart);
		return new CompositeProgressLogger(newTextLogger, newAppLogger);
	}
	
	@Override
	public void logPrepare() {
		if (appLogger != null) appLogger.logPrepare();
		if (textLogger != null) textLogger.logPrepare();		
	}
	
	@Override
	public void logStart() {
		if (appLogger != null) appLogger.logStart();
		if (textLogger != null) textLogger.logStart();		
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
