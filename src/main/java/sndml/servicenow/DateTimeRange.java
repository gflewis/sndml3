package sndml.servicenow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

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

//	@Deprecated
//	public String getName(Interval interval) {
//		return interval.getName(this.getStart());
//	}

}
