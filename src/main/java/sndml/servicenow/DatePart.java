package sndml.servicenow;

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
		assert start.equals(start.truncate(interval));
		assert end.equals(end.truncate(interval));
		assert start.compareTo(end) < 0;
		assert start.incrementBy(interval).equals(end);
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
	
}
