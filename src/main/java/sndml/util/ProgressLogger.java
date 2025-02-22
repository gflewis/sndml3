package sndml.util;

import sndml.agent.JobCancelledException;

public abstract class ProgressLogger {

	protected final DatePartition datePart;
	protected final Metrics metrics;
	
	public ProgressLogger(Metrics metrics, DatePartition datePart) {
		this.datePart = datePart;
		this.metrics = metrics;
	}
	
	public DatePartition getPart() {
		return datePart;
	}
	
	public boolean hasPart() {
		return datePart != null;
	}
	
	public Metrics getMetrics() {
		return metrics;
	}
	
	public abstract ProgressLogger newPartLogger(Metrics newMetrics, DatePartition newPart);	

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
