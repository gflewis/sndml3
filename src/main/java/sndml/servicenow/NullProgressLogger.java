package sndml.servicenow;

import sndml.datamart.DatePart;

/**
 * {@link ProgressLogger} that discards metrics.
 */
public class NullProgressLogger extends ProgressLogger {
	
	final protected DatePart part;
	
	public NullProgressLogger(TableReader reader) {
		this(reader, null);
	}
	
	public NullProgressLogger(TableReader reader, DatePart part) {
		super(reader);
		this.part = part;		
	}
		
	@Override
	public NullProgressLogger newPartLogger(TableReader newReader, DatePart newPart) {
		return new NullProgressLogger(newReader, newPart);
	}
		
	@Override
	public void logPrepare() {	
	}

	@Override
	public void logStart(Integer expected) {
		return;		
	}

	@Override
	public void logProgress() {
		return;		
	}

	@Override
	public void logFinish() {
		return;
	}

	@Override
	public DatePart getPart() {
		return part;
	}


}
