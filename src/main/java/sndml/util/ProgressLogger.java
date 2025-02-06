package sndml.util;

import sndml.agent.JobCancelledException;
import sndml.loader.DatePart;

public abstract class ProgressLogger {

	protected final DatePart datePart;
	protected final Metrics metrics;
	
	public ProgressLogger(Metrics metrics, DatePart datePart) {
		this.datePart = datePart;
		this.metrics = metrics;
	}
	
	public DatePart getPart() {
		return datePart;
	}
	
	public boolean hasPart() {
		return datePart != null;
	}
	
	public Metrics getMetrics() {
		return metrics;
	}
	
	public abstract ProgressLogger newPartLogger(Metrics newMetrics, DatePart newPart);	

	/**
	 * We are starting the initialization process, which includes
	 * calculating the number of expected records that will be processed.
	 */
	public abstract void logPrepare();

	/**
	 * We are starting the actual processing of records.
	 * @throws JobCancelledException
	 */
	public abstract void logStart() throws JobCancelledException;
	
	public abstract void logProgress() throws JobCancelledException;	

	/**
	 * We have completed all processing of record.
	 */
	public abstract void logComplete();
	
}
