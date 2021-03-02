package sndml.servicenow;

public abstract class ProgressLogger {

	protected final TableReader reader;
//	protected final ReaderMetrics readerMetrics;
//	protected final WriterMetrics writerMetrics;
	
	protected String operation = "Processed";
		
	public ProgressLogger() {
		this.reader = null;
//		this.readerMetrics = null;
//		this.writerMetrics = null;
	}
	
	public ProgressLogger(TableReader reader) {
		this.reader = reader;
//		this.readerMetrics = reader.getReaderMetrics();
//		this.writerMetrics = reader.getWriterMetrics();
	}
	
	public ProgressLogger getParent() {
		TableReader parentReader = reader.getParent();
		if (parentReader == null) return null;
		ProgressLogger parentLogger = parentReader.getProgressLogger();
		assert parentLogger != null;
		return parentLogger;		
	}
	
	public ReaderMetrics getReaderMetrics() {		
		return reader==null ? null : reader.getReaderMetrics();
	}
	
	public WriterMetrics getWriterMetrics() {
		return reader==null ? null : reader.getWriterMetrics();
	}
	
	public void setOperation(String operation) {
		this.operation = operation;
	}
	
	public void resetOperation() {
		this.operation = "Processed";
	}
	
	public abstract void logProgress();	
	
}
