package servicenow.api;

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import servicenow.api.InvalidTableNameException;
import servicenow.api.Session;
import servicenow.api.Table;
import servicenow.api.TableSchema;
import servicenow.datamart.AllTests;

public class TableSchemaTest {

	static Logger logger = AllTests.getLogger(TableSchemaTest.class);
	
	@Before
	public void setUp() throws Exception {
		TestingManager.loadDefaultProfile();
	}

	@Test
	public void testReport() throws Exception {
		Session session = TestingManager.getSession();
		Table incident = session.table("incident");		
		TableSchema schema = new TableSchema(incident);
		schema.report(System.out);
	}
	
	@SuppressWarnings("unused")
	@Test(expected = InvalidTableNameException.class)
	public void testBadTable() throws Exception {
		Session session = TestingManager.getSession();
		Table badtable = session.table("incidentxx");
		TableSchema schema = new TableSchema(badtable);
		fail();
	}

}
