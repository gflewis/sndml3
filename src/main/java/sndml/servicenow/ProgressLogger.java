package sndml.servicenow;

public abstract class ProgressLogger {

//	protected String name;
	protected String operation = "Processed";
			
//	public String getName() { return this.name; }
				
	public void setOperation(String operation) {
		this.operation = operation;
	}
	
	public void resetOperation() {
		this.operation = "Processed";
	}
	
	public abstract void logStart(TableReader reader, String operation);
	
	public abstract void logProgress(TableReader reader);	
	
	public abstract void logFinish(TableReader reader);
	
}
