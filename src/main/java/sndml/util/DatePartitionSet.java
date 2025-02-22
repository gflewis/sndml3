package sndml.util;

/**
 * A {@link DatePartitionSet} divides a {@link DateTimeRange} into partitions 
 * based on the {@link PartitionInterval}.
 * <p>
 * Each {@link DatePartition} is a {@link DateTimeRange} which begins and ends on an interval boundary.
 * Thus, the first and last partitions may exceed the boundaries of the original range.
 * <p>
 * The {@link #iterator()} method returns the partitions in reverse chronological order.
 * In other words, the most recent partition is returned first.
 */
public class DatePartitionSet implements Iterable<DatePartition> {
	
	private final DateTimeRange range;
	private final PartitionInterval interval;

	public DatePartitionSet(DateTimeRange range, PartitionInterval interval) {
		this.range = range;
		this.interval = interval;
		if (range == null) {
			// this is empty
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
	
	public PartitionInterval getInterval() {
		return this.interval;			
	}

	/**
	 * Compute the number of partitions in this {@link DatePartitionSet}.
	 * 
	 * @return number of partitions
	 */
	public int computeSize() {
		if (range == null) return 0;
		if (range.start == null) return 0;
		if (range.end == null ) return 0;
		if (range.end.compareTo(range.start) <= 0) return 0; // end is before start (should be impossible)
		if (interval == null) return 0;
		DateTime end = range.end.ceiling(interval);
		assert end.compareTo(range.end) >= 0 : "computeSize bad ceiling";
		DateTime start = end.decrementBy(interval);
		assert start.compareTo(end) < 0 : "computeSize bad decrement";
		int size = 1;
		while (start.compareTo(range.start) > 0) {
			size += 1;
			end = start;
			start = end.decrementBy(interval);
			assert start.compareTo(end) < 0;
		}
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
		return new DatePartitionIterator(this);
	}

}
