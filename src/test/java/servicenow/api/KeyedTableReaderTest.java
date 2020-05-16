package servicenow.api;

import org.junit.Test;

public class KeyedTableReaderTest {

	Session session;
	Table table;
	SoapTableAPI impl;
	
	@Test
	public void test() throws Exception {
		session = TestingManager.setDefaultProfile().getSession();
		table = session.table("incident");
		SoapKeySetTableReader reader = new SoapKeySetTableReader(table);
		reader.setFilter(EncodedQuery.all());
		reader.setWriter(new NullWriter());
		reader.setPageSize(20).initialize();
		reader.call();
	}

}
