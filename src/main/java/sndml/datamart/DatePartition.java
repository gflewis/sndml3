package sndml.datamart;

import java.util.Iterator;

import sndml.servicenow.DateTimeRange;

public class DatePartition implements Iterable<DatePart> {
	
	private final DateTimeRange range;
	private final Interval interval;

	public DatePartition(DateTimeRange range, Interval interval) {
		assert range != null;
		assert interval != null;
		if (range.getStart() == null)
			throw new IllegalArgumentException("start date is null");
		if (range.getEnd() == null)
			throw new IllegalArgumentException("end date is null");
		if (range.getEnd().compareTo(range.getStart()) < 0)
			throw new IllegalArgumentException("end date is before start date");
		this.range = range;
		this.interval = interval;
	}
	
	public DateTimeRange getRange() {
		return this.range;
	}
	
	public Interval getInterval() {
		return this.interval;			
	}
		
	public String toString() {
		return range.toString() + " by " + interval.toString();
	}
	
	@Override
	/**
	 * Process the ranges in a partition beginning with the most recent
	 * and ending with the earliest.
	 */
	public Iterator<DatePart> iterator() {
		return new DatePartitionIterator(this.range, this.interval);
	}

}
