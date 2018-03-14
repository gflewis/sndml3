package servicenow.api;

import java.io.File;
import org.junit.Test;

public class KeyedTableReaderTest {

	Session session;
	Table table;
	SoapTableAPI impl;
	
	@Test
	public void test() throws Exception {
		String filename = "/tmp/incident.sni";
		TestingManager.loadDefaultProfile();
		session = TestingManager.getSession();
		table = session.table("incident");
		FileWriter writer = new FileWriter(new File(filename));
		SoapKeySetTableReader reader = new SoapKeySetTableReader(table);
		reader.setFilter(EncodedQuery.all());
		reader.setWriter(writer);
		reader.setPageSize(20).initialize();
		reader.call();
	}

}
