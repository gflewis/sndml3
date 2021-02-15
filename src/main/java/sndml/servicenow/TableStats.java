package sndml.servicenow;

public class TableStats {
	
	public int count;
	public DateTimeRange created;
	
	public int getCount() {
		return count;
	}
	
	public TableStats setCount(int count) {
		this.count = count;
		return this;
	}
	
	public TableStats setCreated(DateTimeRange range) {
		this.created = range;
		return this;
	}
	
	public DateTimeRange getCreated() {
		return this.created;
	}

}
