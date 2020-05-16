package servicenow.api;

public class KeySetTableReaderFactory extends TableReaderFactory {
	
	public KeySetTableReaderFactory(Table table, RecordWriter writer) {
		super(table, writer);
	}
	
	public KeySetTableReader createReader() {
		KeySetTableReader reader = new KeySetTableReader(this.table);
		configure(reader);
		return reader;
	}

}
