package sndml.servicenow;

import java.util.Iterator;

public class DatePartition implements Iterable<DateTimeRange> {
	
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
//		return String.format("%s[interval=%s size=%d min=%s max=%s]", 
//				this.getClass().getSimpleName(), interval.toString(), size, oldest.getStart(), newest.getEnd());
		return range.toString() + " by " + interval.toString();
	}
	

	static public String partName(Interval interval, DateTimeRange partRange) {
		String prefix = interval.toString().substring(0,1);
		String datepart = partRange.getStart().toString();
		if (interval == Interval.HOUR) {
			if (datepart.length() == DateTime.DATE_ONLY) datepart += " 00:00:00";
			datepart = datepart.replaceAll(" ", "T");
		}
		return prefix + datepart;
	}

	@Override
	/**
	 * Process the ranges in a partition beginning with the most recent
	 * and ending with the earliest.
	 */
	public Iterator<DateTimeRange> iterator() {
		return new DatePartitionIterator(this.range, this.interval);
	}

}
