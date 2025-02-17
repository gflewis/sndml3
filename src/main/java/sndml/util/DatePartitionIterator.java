package sndml.util;

import java.util.Iterator;

/**
 * Iterate through the ranges of a partition backwards, starting with
 * the most recent, and ending with the earliest.
 *
 */
public class DatePartitionIterator implements Iterator<DatePart> {
	
	final DateTimeRange range;
	final IntervalSize interval;
	final DatePart newest;
	final DatePart oldest;
	final int size;
	boolean started = false;
	boolean finished = false;
	DatePart current = null;

	public DatePartitionIterator(DateTimeRange range, IntervalSize interval) {
		this.range = range;
		this.interval = interval;
		if (range == null) {
			this.newest = null;
			this.oldest = null;
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
				this.newest = null;
				this.oldest = null;
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
				this.newest = new DatePart(interval, start, end);
				int size = 1;
				while (start.compareTo(range.getStart()) > 0) {
					size += 1;
					end = start;
					start = end.decrementBy(interval);
					assert start.compareTo(end) < 0;
				}
				this.oldest = new DatePart(interval, start, end);
				this.size = size;
			}			
		}
	}

	public int getSize() {
		return this.size;
	}
	
	@Override
	public boolean hasNext() {
		return (current == null) ? size > 0 :
			current.getStart().compareTo(range.getStart()) > 0;
	}

	@Override
	public DatePart next() {
		if (finished) {
			return null;
		}
		if (!started) { 
			assert newest != null : "newest is null";
			current = newest;			
			started = true;
			return current;
		}
		DateTime end = current.getStart();
		DateTime start = end.decrementBy(interval);
		assert start.compareTo(end) < 0 : "start not before end"; 
		if (end.compareTo(oldest.start) <= 0) {
			finished = true;
			return null;			
		}		
		current = new DatePart(interval, start, end);
		assert current != null;
		return current;
	}

}
