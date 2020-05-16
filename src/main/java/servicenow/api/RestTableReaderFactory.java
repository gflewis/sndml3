package servicenow.api;

public class RestTableReaderFactory extends TableReaderFactory {

	public RestTableReaderFactory(Table table, RecordWriter writer) {
		super(table, writer);
	}
	
	public RestTableReader createReader() {
		RestTableReader reader = new RestTableReader(table);
		configure(reader);
		reader.enableStats(true);
		return reader;
	}
	
}
