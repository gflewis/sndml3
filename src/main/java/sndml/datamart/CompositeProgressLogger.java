package sndml.datamart;

import sndml.servicenow.ProgressLogger;
import sndml.servicenow.ReaderMetrics;
import sndml.servicenow.Log4jProgressLogger;
import sndml.servicenow.WriterMetrics;

public class CompositeProgressLogger extends ProgressLogger {

	final Log4jProgressLogger slf4jLogger;
	final AppRunLogger appLogger;
		
	public CompositeProgressLogger(@SuppressWarnings("rawtypes") Class cls, AppRunLogger appLogger) {
		super();
		this.slf4jLogger = new Log4jProgressLogger(cls);
		this.appLogger = appLogger; 		
	}

	@Override
	public void logProgress(ReaderMetrics readerMetrics, WriterMetrics writerMetrics) {
		if (appLogger != null) appLogger.logProgress(readerMetrics, writerMetrics);
		if (slf4jLogger != null) slf4jLogger.logProgress(readerMetrics, writerMetrics);
	}

}
