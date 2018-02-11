package servicenow.rest;

import java.io.IOException;

import servicenow.core.*;

public class DatePartReader extends RestTableReader {

	DateTimeRange partRange;
	String partName;
	
	public DatePartReader(MultiDatePartReader parent, String partName, DateTimeRange partRange) {
		super(parent.table);
		assert parent != null;
		assert partName != null;
		assert partRange != null;
		this.partRange = partRange;
		this.partName = partName;
		this.setPageSize(parent.getPageSize());
		this.setBaseQuery(parent.getBaseQuery());
		this.setUpdatedRange(parent.getUpdatedRange());
		this.setCreatedRange(partRange.intersect(parent.getCreatedRange()));
		this.setParent(parent);
		this.setWriter(parent.getWriter());
	}
	
	public void initialize() throws IOException {
		super.initialize();		
	}

	public String getName() {
		return this.partName;
	}
	
	@Override
	public void setLogContext() {
		Log.setContext(table, writer.getWriterName());
		Log.setPartitionContext(partName);
	}
		
}
