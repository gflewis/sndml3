package sndml.util;

/**
 * One entry of a {@link DatePartitions}. 
 * Both start and end must be on an {@link IntervalSize} boundary.
 *
 */
public class Partition extends DateTimeRange {

	protected final IntervalSize interval;
	
	public Partition(IntervalSize interval, DateTime start, DateTime end) {
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
	
	public DateTimeRange getRange() {
		return new DateTimeRange(this.start, this.end);
	}

	static public String getName(IntervalSize interval, DateTime start) {
		String prefix = (interval.equals(IntervalSize.FIVE_MINUTE) || interval.equals(IntervalSize.MINUTE)) ? 
				"" : interval.toString().substring(0,1);
		switch (interval) {
		case YEAR: 
			return prefix + "-" + start.toString().substring(0, 4);
		case QUARTER:
		case MONTH:
			return prefix + "-" + start.toString().substring(0, 7);
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
