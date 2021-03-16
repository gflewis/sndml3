package sndml.servicenow;

import sndml.datamart.DatePart;

public abstract class ProgressLogger {

	protected final TableReader reader;
	protected final DatePart datePart;	
								
	public ProgressLogger(TableReader reader) {
		this(reader, null);
	}

	public ProgressLogger(TableReader reader, DatePart part) {
		this.reader = reader;
		this.datePart = part;
	}

	public TableReader getReader() {
		return this.reader;
	}
	
	public DatePart getPart() {
		return datePart;
	}
	
	public boolean hasPart() {
		return datePart != null;
	}
		
	public abstract ProgressLogger newPartLogger(TableReader newReader, DatePart newPart);	

	/**
	 * We are starting the initialization process, which includes
	 * calculating the number of expected records that will be processed.
	 */
	public abstract void logPrepare();

	/**
	 * We are starting the actuall processing of records.
	 */
	public abstract void logStart(Integer expected);
	
	public abstract void logProgress();	

	/**
	 * We have completed all processing of record.
	 */
	public abstract void logFinish();
	
}
