package sndml.datamart;

import sndml.servicenow.*;

public class CompositeProgressLogger extends ProgressLogger {

	final Log4jProgressLogger slf4jLogger;
	final AppRunLogger appLogger;
	
	@Deprecated
	public CompositeProgressLogger(
			TableReader reader, 
			@SuppressWarnings("rawtypes") Class cls, 
			AppRunLogger appLogger) {
		super();
		this.slf4jLogger = new Log4jProgressLogger(reader, cls);
		this.appLogger = appLogger; 		
	}
	
	public CompositeProgressLogger(TableReader reader, AppRunLogger appLogger) {
		super();
		this.slf4jLogger = new Log4jProgressLogger(reader, reader.getClass());
		this.appLogger = appLogger; 		
	}

	@Override
	public void logProgress() {
		if (appLogger != null) appLogger.logProgress();
		if (slf4jLogger != null) slf4jLogger.logProgress();
	}

//	@Override
//	public void logPartProgress(
//			String partName, 
//			ReaderMetrics readerMetrics, 
//			WriterMetrics writerMetrics) {
//		if (appLogger != null) appLogger.logPartProgress(partName, readerMetrics, writerMetrics);
//		if (slf4jLogger != null) slf4jLogger.logPartProgress(partName, readerMetrics, writerMetrics);
//		
//	}

}
