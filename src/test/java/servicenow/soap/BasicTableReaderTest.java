package servicenow.soap;

import java.io.File;
import org.junit.Test;

import servicenow.core.*;
import servicenow.soap.SoapTableReader;
import servicenow.soap.SoapTableAPI;

public class BasicTableReaderTest {

	Session session;
	Table table;
	SoapTableAPI impl;
	
	@Test
	public void test() throws Exception {
		String filename = "/tmp/incident.sni";
		TestingManager.loadDefaultProfile();
		session = TestingManager.getSession();
		table = session.table("incident");
		impl = table.soap();		
		FileWriter writer = new FileWriter(new File(filename));
		SoapTableReader reader = new SoapTableReader(impl);
		reader.setBaseQuery(EncodedQuery.all());
		reader.setWriter(writer);
		reader.setPageSize(20).initialize();
		reader.call();
	}

}
