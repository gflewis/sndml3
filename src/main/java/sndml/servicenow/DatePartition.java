package sndml.servicenow;

import java.util.Iterator;

public class DatePartition implements Iterable<DateTimeRange> {
	
	private final DateTimeRange range;
	private final Interval interval;
//	private final int size;
//	private final DateTimeRange oldest;
//	private final DateTimeRange newest;

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
//		int size = 0;
//		DateTimeRange oldest = null;
//		DateTimeRange newest = null;
//		for (DateTimeRange part : this) {
//			size += 1;
//			if (newest == null) newest = part;
//			oldest = part;
//		}
//		this.size = size;
//		this.oldest = oldest;
//		this.newest = newest;					
	}
	
	public DateTimeRange getRange() {
		return this.range;
	}
	
	public Interval getInterval() {
		return this.interval;			
	}
	
//	public DateTimeRange getNewest() {
//		return oldest;
//	}
//	
//	public DateTimeRange getOldest() {
//		return newest;
//	}
//	
//	public int size() {
//		return size;
//	}
	
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
	 * Process partitions beginning with the most recent
	 */
	public Iterator<DateTimeRange> iterator() {
		return new DateTimeBackwardsIterator(this.range, this.interval);
	}

}
