package sndml.servicenow;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.InvalidTableNameException;
import sndml.servicenow.Session;
import sndml.servicenow.Table;
import sndml.servicenow.TableSchema;

public class TableSchemaTest {

	static Logger logger = LoggerFactory.getLogger(TableSchemaTest.class);
	
	public TableSchemaTest() {
		TestingManager.setDefaultProfile(this.getClass());		
	}
	
	@Test
	public void testReport() throws Exception {
		Session session = TestingManager.getProfile().getSession();
		Table incident = session.table("incident");		
		TableSchema schema = new TableSchema(incident);
		schema.report(System.out);
	}
	
	@SuppressWarnings("unused")
	@Test(expected = InvalidTableNameException.class)
	public void testBadTable() throws Exception {
		Session session = TestingManager.getProfile().getSession();
		Table badtable = session.table("incidentxx");
		TableSchema schema = new TableSchema(badtable);
		fail();
	}

}
