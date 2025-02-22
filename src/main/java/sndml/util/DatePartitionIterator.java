package sndml.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterate through the ranges of a partition backwards, starting with
 * the most recent, and ending with the earliest.
 *
 */
public class DatePartitionIterator implements Iterator<DatePartition> {
	
	final DatePartitionSet parts;
	final DateTimeRange range;
	final PartitionInterval interval;
	final int size;
	boolean started = false;
	boolean finished = false;
	DatePartition current = null;

	public DatePartitionIterator(DatePartitionSet parts) {
		this.parts = parts;
		this.range = parts.getRange();
		this.interval = parts.getInterval();
		if (parts.isEmpty()) {
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
				this.size = parts.computeSize();
			}
		}
  	}
	
	@Deprecated
	public DatePartitionIterator(DateTimeRange range, PartitionInterval interval) {
		this.range = range;
		this.interval = interval;
		this.parts = new DatePartitionSet(range, interval);
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
	private DatePartition getNewest() {
		DateTime end = range.end.ceiling(interval);
		assert end.compareTo(range.end) >= 0 : "getNewest bad ceiling";
		DateTime start = end.decrementBy(interval);
		return new DatePartition(interval, start, end);		
	}

	/**
	 * Return the earliest part
	 */
	@SuppressWarnings("unused")
	private DatePartition getOldest() {
		DateTime start = range.start.truncate(interval);
		assert start.compareTo(range.start) <= 0 : "getOldest bad truncate"; 
		DateTime end = start.incrementBy(interval);
		return new DatePartition(interval, start, end); 
	}
		
	
	public int getSize() {
		return this.size;
	}
	
	@Override
	public boolean hasNext() {		
		return !finished;
	}

	@Override
	public DatePartition next() throws NoSuchElementException {
		if (finished) throw new NoSuchElementException();
		if (!started) { 
			current = getNewest();			
			started = true;
		}
		else {
			DateTime end = current.start;
			DateTime start = end.decrementBy(interval);
			assert start.compareTo(end) < 0 : "start not before end"; 
			current = new DatePartition(interval, start, end);			
		}
		if (current.start.compareTo(range.start) <= 0) finished = true;
		return current;
	}

}
