package sndml.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * A {@link DateTime} range which is open at the beginning but closed at the end.
 * In other words,
 * the start date is included but the end date is NOT included.
 */
public class DateTimeRange {

	protected final DateTime start;
	protected final DateTime end;
	
	public DateTimeRange(DateTime start, DateTime end) {
		this.start = start;
		this.end = end;
	}
	
	public DateTimeRange(String start, String end) {
		assert end != null : "null end invalid for constructor with Strings";
		assert start != null : "null start invalid for constructor with Strings";
		this.start = new DateTime(start);
		this.end = new DateTime(end);
	}
	
	public static DateTimeRange all() {
		return new DateTimeRange((DateTime) null, (DateTime) null);
	}
	
	/**
	 * Determine whether a date falls within the {@link DateTimeRange}.
	 * The range is open at the beginning and closed at the end.
	 * In other words,
	 * the start date is included but the end date is NOT included.
	 */
	public boolean contains(DateTime date) {
		assert date != null : "null date cannot be tested";
		if (start != null && date.compareTo(start) < 0) return false;
		if (end != null && date.compareTo(end) >= 0) return false;
		return true;
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
	
	@Override
	public boolean equals(Object obj) {
		DateTimeRange other = (DateTimeRange) obj;
		return start.equals(other.start) && end.equals(other.end); 
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
		
	public String toString() {
		return String.format("[%s,%s]", 
				hasStart() ? start.toString() : "null", 
				hasEnd() ? end.toString() : "null");
	}
	
	public ArrayNode toJsonNode() {
		ObjectMapper mapper = new ObjectMapper();		
		ArrayNode node = mapper.createArrayNode();
		node.add(hasStart() ? start.toString() : null);
		node.add(hasEnd() ? end.toString() : null);
		return node;
	}

}
