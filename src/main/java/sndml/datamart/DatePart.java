package sndml.datamart;

import sndml.servicenow.DateTime;
import sndml.servicenow.DateTimeRange;

/**
 * One piece of a {@link DatePartition}. 
 * Both start and end must be on an {@link Interval} boundary.
 *
 */
public class DatePart extends DateTimeRange {

	protected final Interval interval;
	
	public DatePart(Interval interval, DateTime start, DateTime end) {
		super(start, end);
		this.interval = interval;
		assert interval != null;
		assert start != null;
		assert end != null;
		assert start.compareTo(end) < 0;
		assert start.equals(start.truncate(interval)) : String.format("DatePart.start=%s", start);
		assert end.equals(end.truncate(interval)) : String.format("DatePart end=%s", end);
		assert start.incrementBy(interval).equals(end) :
			String.format("DatePart start=%s end=%s", start, end);
	}
	
	public String getName() {
		return getName(interval, start);
	}

	static public String getName(Interval interval, DateTime start) {
		String prefix = (interval.equals(Interval.FIVEMIN) || interval.equals(Interval.MINUTE)) ? 
				"" : interval.toString().substring(0,1);
		switch (interval) {
		case YEAR: 
			return prefix + start.toString().substring(0, 4);
		case QUARTER:
		case MONTH:
		case WEEK:
		case DAY:
			return prefix + start.toString().substring(0, 10);
		default: 
			return prefix + start.toFullString().replaceAll(" ", "T");
		}
	}
	
	@Override
	public String toString() {
		return getName();
	}
	
}
