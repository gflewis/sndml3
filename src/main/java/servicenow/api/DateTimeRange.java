package servicenow.api;

public class DateTimeRange {

	protected final DateTime start;
	protected final DateTime end;
	
	public DateTimeRange(DateTime start, DateTime end) {
		this.start = start;
		this.end = end;
	}
	
	public static DateTimeRange all() {
		return new DateTimeRange(null, null);
	}
	
	public DateTime getStart() {
		return start;
	}
	
	public DateTime getEnd() {
		return end;
	}
	
	public boolean hasStart() {
		return start != null;
	}
	
	public boolean hasEnd() {
		return end != null;
	}

	/**
	 * Determine the overlap of two date ranges
	 */
	public DateTimeRange intersect(DateTimeRange other) {
		DateTime start = 
			other != null && other.hasStart() && (
				this.getStart() == null || other.getStart().after(this.getStart())) ? 
			other.getStart() : this.getStart();
		DateTime end = 
			other != null && other.hasEnd() && (
				this.getEnd() == null || other.getEnd().before(this.getEnd())) ? 
			other.getEnd() : this.getEnd();
		return new DateTimeRange(start, end);		
	}
	
	EncodedQuery asQuery(String fieldname) {
		EncodedQuery result = new EncodedQuery();
		if (start != null) result.addQuery(fieldname, EncodedQuery.GREATER_THAN_OR_EQUALS, start.toString());
		if (end != null) result.addQuery(fieldname, EncodedQuery.LESS_THAN, end.toString());
		return result;
	}
	
	public String toString() {
		return String.format("[%s,%s]", 
				hasStart() ? start.toString() : "null", 
				hasEnd() ? end.toString() : "null");
	}
		
}
