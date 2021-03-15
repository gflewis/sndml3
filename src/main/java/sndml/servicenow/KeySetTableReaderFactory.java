package sndml.servicenow;

public class KeySetTableReaderFactory extends TableReaderFactory {

	public KeySetTableReaderFactory(Table table) {
		super(table);
	}
	
	public KeySetTableReader createReader() {
		KeySetTableReader reader = new KeySetTableReader(this.table);
		configure(reader);
		return reader;
	}

}
