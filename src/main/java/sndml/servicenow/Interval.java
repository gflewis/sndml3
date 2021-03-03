package sndml.servicenow;

public enum Interval {YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, FIVEMIN, MINUTE;

	/**
	 * Return the name of an interval starting on a date
	 */
	public String getName(DateTime start) {
		return getName(this, start);
	}
	
	static public String getName(Interval interval, DateTime start) {
		String prefix = (interval.equals(FIVEMIN) || interval.equals(MINUTE)) ? 
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
