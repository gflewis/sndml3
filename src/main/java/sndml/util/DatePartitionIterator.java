package sndml.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterate through the ranges of a partition backwards, starting with
 * the most recent, and ending with the earliest.
 *
 */
public class DatePartitionIterator implements Iterator<Partition> {
	
	final DatePartitions partition;
	final DateTimeRange range;
	final IntervalSize interval;
	final int size;
	boolean started = false;
	boolean finished = false;
	Partition current = null;

	public DatePartitionIterator(DatePartitions partition) {
		this.partition = partition;
		this.range = partition.getRange();
		this.interval = partition.getInterval();
		if (partition.isEmpty()) {
			this.size = 0;
			this.finished = true;			
		}
		else {
			if (range.start == null)
				throw new IllegalArgumentException("start date is null");
			if (range.end == null)
				throw new IllegalArgumentException("end date is null");
			if (range.end.compareTo(range.start) < 0)
				throw new IllegalArgumentException("end date is before start date");
			if (range.end.compareTo(range.start) == 0) {
				this.size = 0;
				this.finished = true;
			}
			else {
				assert interval != null : "interval is null";
				assert range.end != null : "end date is null";
				assert range.start != null : "start date is null";
				this.size = partition.computeSize();
			}
		}
  	}
	
	@Deprecated
	public DatePartitionIterator(DateTimeRange range, IntervalSize interval) {
		this.range = range;
		this.interval = interval;
		this.partition = new DatePartitions(range, interval);
		if (range == null) {
			this.size = 0;
			this.finished = true;
		}
		else {
			if (range.getStart() == null)
				throw new IllegalArgumentException("start date is null");
			if (range.getEnd() == null)
				throw new IllegalArgumentException("end date is null");
			if (range.getEnd().compareTo(range.getStart()) < 0)
				throw new IllegalArgumentException("end date is before start date");
			if (range.getEnd().compareTo(range.getStart()) == 0) {
				this.size = 0;
				this.finished = true;
			}
			else {
				assert interval != null : "interval is null";
				assert range.end != null : "end date is null";
				assert range.start != null : "start date is null";
				
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
				this.size = size;
			}			
		}
	}

	/**
	 * Return the most recent part
	 */
	private Partition getNewest() {
		DateTime end = range.end.ceiling(interval);
		assert end.compareTo(range.end) >= 0 : "getNewest bad ceiling";
		DateTime start = end.decrementBy(interval);
		return new Partition(interval, start, end);		
	}

	/**
	 * Return the earliest part
	 */
	@SuppressWarnings("unused")
	private Partition getOldest() {
		DateTime start = range.start.truncate(interval);
		assert start.compareTo(range.start) <= 0 : "getOldest bad truncate"; 
		DateTime end = start.incrementBy(interval);
		return new Partition(interval, start, end); 
	}
		
	
	public int getSize() {
		return this.size;
	}
	
	@Override
	public boolean hasNext() {		
		return !finished;
	}

	@Override
	public Partition next() throws NoSuchElementException {
		if (finished) throw new NoSuchElementException();
		if (!started) { 
			current = getNewest();			
			started = true;
		}
		else {
			DateTime end = current.start;
			DateTime start = end.decrementBy(interval);
			assert start.compareTo(end) < 0 : "start not before end"; 
			current = new Partition(interval, start, end);			
		}
		if (current.start.compareTo(range.start) <= 0) finished = true;
		return current;
	}

}
