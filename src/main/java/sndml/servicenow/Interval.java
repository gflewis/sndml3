package sndml.servicenow;

import java.util.EnumSet;

public enum Interval {YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, FIVEMIN, MINUTE;

	/**
	 * Return the name of an interval starting on a date
	 */
	public String getName(DateTime start) {
		return getName(this, start);
	}
	
	static public String getName(Interval interval, DateTime start) {
		EnumSet<Interval> smallRange = EnumSet.range(Interval.HOUR, Interval.MINUTE);
		if (smallRange.contains(interval)) {
			return start.toString().replaceAll(" ", "T");
		}
		else {
			String prefix = interval.toString().substring(0,1);			
			return prefix + start.toString();
		}
	}

}
