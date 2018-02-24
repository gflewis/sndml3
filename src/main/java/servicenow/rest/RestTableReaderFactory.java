package servicenow.rest;

import servicenow.core.*;

public class RestTableReaderFactory extends TableReaderFactory {

	public RestTableReaderFactory(Table table, Writer writer) {
		super(table, writer);
	}
	
	public RestTableReader createReader() {
		RestTableReader reader = new RestTableReader(table);
		reader.enableStats(true);
		reader.setBaseQuery(baseQuery);
		reader.setUpdatedRange(updatedRange);
		reader.setCreatedRange(createdRange);
		reader.setPageSize(pageSize);
		reader.setWriter(writer);
		reader.setReaderName(readerName);
		return reader;
	}
	
}
