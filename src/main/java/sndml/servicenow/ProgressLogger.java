package sndml.servicenow;

public abstract class ProgressLogger {

	protected String operation = "Processed";
		
	public ProgressLogger() {
	}
	
	public void setOperation(String operation) {
		this.operation = operation;
	}
	
	public void resetOperation() {
		this.operation = "Processed";
	}
	
	public abstract void logProgress(ReaderMetrics readerMetrics, WriterMetrics writerMetrics);
}
