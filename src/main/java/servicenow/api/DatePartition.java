package servicenow.api;

import java.util.ArrayList;

import servicenow.api.DateTime.Interval;

public class DatePartition extends ArrayList<DateTimeRange> {

	private static final long serialVersionUID = 1L;
	
	final DateTime.Interval interval;

	public DatePartition(DateTimeRange range, DateTime.Interval interval) {
		super();
		assert range != null;
		assert interval != null;
		this.interval = interval;		
		if (range.getStart() == null)
			throw new IllegalArgumentException("start date is null");
		if (range.getEnd() == null)
			throw new IllegalArgumentException("end date is null");
		if (range.getEnd().compareTo(range.getStart()) < 0)
			throw new IllegalArgumentException("end date is before start date");
		DateTime start = range.getStart().truncate(interval);
		DateTime end;
		do {
			end = start.incrementBy(interval);
			assert end.compareTo(start) > 0;
			this.add(new DateTimeRange(start, end));
			start = end;
		} while (range.getEnd().compareTo(end) > 0);
	}
	
	public DateTime.Interval getInterval() {
		return interval;
	}
	
	public String toString() {
		DateTimeRange first = get(0);
		DateTimeRange last = get(size() - 1);
		return String.format("%s[interval=%s size=%d min=%s max=%s]", 
				this.getClass().getSimpleName(), interval.toString(), size(), first.getStart(), last.getEnd());
	}

	static public String partName(DateTime.Interval interval, DateTimeRange partRange) {
		String prefix = interval.toString().substring(0,1);
		String suffix = partRange.getStart().toString();
		if (interval == Interval.HOUR) {
			if (suffix.length() == DateTime.DATE_ONLY) suffix += " 00:00:00";
			suffix.replaceAll(" ", "T");
		}
		return prefix + suffix;
	}
}
