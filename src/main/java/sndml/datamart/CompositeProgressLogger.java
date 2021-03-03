package sndml.datamart;

import sndml.servicenow.*;

public class CompositeProgressLogger extends ProgressLogger {

	final Log4jProgressLogger log4jLogger;
	final AppRunLogger appLogger;
		
//	public CompositeProgressLogger(Log4jProgressLogger log4jLogger, AppRunLogger appLogger) {
//		super(log4jLogger.getName(), log4jLogger.getReader());
//		this.log4jLogger = log4jLogger;
//		this.appLogger = appLogger;		
//	}
		
	public CompositeProgressLogger(TableReader reader, AppRunLogger appLogger) {
		this.log4jLogger = new Log4jProgressLogger(reader);
		this.appLogger = appLogger; 		
	}

	@Override
	public void logStart(TableReader reader, String operation) {
		if (appLogger != null) appLogger.logStart(reader, operation);
		if (log4jLogger != null) log4jLogger.logStart(reader, operation);		
	}
	
	@Override
	public void logProgress(TableReader reader) {
		if (appLogger != null) appLogger.logProgress(reader);
		if (log4jLogger != null) log4jLogger.logProgress(reader);
	}


	@Override
	public void logFinish(TableReader reader) {
		if (appLogger != null) appLogger.logFinish(reader);
		if (log4jLogger != null) log4jLogger.logFinish(reader);
		
	}


}
