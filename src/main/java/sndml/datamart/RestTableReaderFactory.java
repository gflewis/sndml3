package sndml.datamart;

import sndml.servicenow.RestTableReader;
import sndml.servicenow.Table;

public class RestTableReaderFactory extends TableReaderFactory {

	public RestTableReaderFactory(Table table) {
		super(table);
	}
	
	public RestTableReader createReader() {
		RestTableReader reader = new RestTableReader(table);
		configure(reader);
		reader.enableStats(true);
		return reader;
	}
	
}
