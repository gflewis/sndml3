package sndml.util;

public class DatePartition implements Iterable<DatePart> {
	
	private final DateTimeRange range;
	private final IntervalSize interval;

	public DatePartition(DateTimeRange range, IntervalSize interval) {
		this.range = range;
		this.interval = interval;
		if (range == null) {
			// creating an empty partition
		}
		else {
			if (range.getStart() == null)
				throw new IllegalArgumentException("start date is null");
			if (range.getEnd() == null)
				throw new IllegalArgumentException("end date is null");
			if (range.getEnd().compareTo(range.getStart()) < 0)
				throw new IllegalArgumentException("end date is before start date");			
		}
	}
		
	public boolean isEmpty() {
		return range == null;
	}
	
	public DateTimeRange getRange() {
		return this.range;
	}
	
	public IntervalSize getInterval() {
		return this.interval;			
	}

	public int computeSize() {
		if (range == null) return 0;
		if (range.start == null) return 0;
		if (range.end == null ) return 0;
		if (range.end.compareTo(range.start) <= 0) return 0; // end is before start
		if (interval == null) return 0;
		DateTime end = range.getEnd().truncate(interval);
		if (end.compareTo(range.getEnd()) < 0) end = end.incrementBy(interval);
		assert end.compareTo(range.getEnd()) >= 0;
		DateTime start = end.decrementBy(interval);
		assert start.compareTo(end) < 0;
		int size = 1;
		while (start.compareTo(range.getStart()) > 0) {
			size += 1;
			end = start;
			start = end.decrementBy(interval);
			assert start.compareTo(end) < 0;
		}
		new DatePart(interval, start, end);
		return size;
		
	}
	
	public String toString() {
		if (range == null)
			return "empty";
		else
			return range.toString() + " by " + interval.toString();
	}
	
	@Override
	/**
	 * Process the ranges in a partition beginning with the most recent
	 * and ending with the earliest.
	 */
	public DatePartitionIterator iterator() {
		return new DatePartitionIterator(this.range, this.interval);
	}

}
