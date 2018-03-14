package servicenow.api;

public class KeySetTableReaderFactory extends TableReaderFactory {
	
	public KeySetTableReaderFactory(Table table, Writer writer) {
		super(table, writer);
	}
	
	public KeySetTableReader createReader() {
		KeySetTableReader reader = new KeySetTableReader(this.table);
		reader.setFilter(filter);
		reader.setUpdatedRange(updatedRange);
		reader.setCreatedRange(createdRange);
		reader.setOrderBy(orderBy);
		reader.setWriter(writer);
		reader.setReaderName(readerName);
		return reader;
	}

}
