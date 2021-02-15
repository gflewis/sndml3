package sndml.datamart;

import sndml.servicenow.ProgressLogger;
import sndml.servicenow.ReaderMetrics;
import sndml.servicenow.SLF4jProgressLogger;
import sndml.servicenow.WriterMetrics;

public class CompositeProgressLogger extends ProgressLogger {

	final SLF4jProgressLogger slf4jLogger;
	final AppRunLogger appLogger;
	
//	public CompositeProgressLogger(SLF4jProgressLogger slf4jLogger, AppRunLogger appLogger) {
//		super();
//		this.slf4jLogger = slf4jLogger;
//		this.appLogger = appLogger;
//	}
	
	public CompositeProgressLogger(@SuppressWarnings("rawtypes") Class clazz, AppRunLogger appLogger) {
		super();
		this.slf4jLogger = new SLF4jProgressLogger(clazz);
		this.appLogger = appLogger; 		
	}

	@Override
	public void logProgress(ReaderMetrics readerMetrics, WriterMetrics writerMetrics) {
		if (appLogger != null) appLogger.logProgress(readerMetrics, writerMetrics);
		if (slf4jLogger != null) slf4jLogger.logProgress(readerMetrics, writerMetrics);
	}

}
