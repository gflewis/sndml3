package servicenow.core;

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import servicenow.core.InvalidTableNameException;
import servicenow.core.Session;
import servicenow.core.Table;
import servicenow.core.TableSchema;
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
