package servicenow.api;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSchemaTest {

	static Logger logger = LoggerFactory.getLogger(TableSchemaTest.class);
	
	@Test
	public void testReport() throws Exception {
		Session session = TestingManager.setDefaultProfile().getSession();
		Table incident = session.table("incident");		
		TableSchema schema = new TableSchema(incident);
		schema.report(System.out);
	}
	
	@SuppressWarnings("unused")
	@Test(expected = InvalidTableNameException.class)
	public void testBadTable() throws Exception {
		Session session = TestingManager.setDefaultProfile().getSession();
		Table badtable = session.table("incidentxx");
		TableSchema schema = new TableSchema(badtable);
		fail();
	}

}
