package sndml.datamart;

import sndml.servicenow.KeySetTableReader;
import sndml.servicenow.Table;

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
