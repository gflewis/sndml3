package sndml.servicenow;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSchemaTest {

	Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public TableSchemaTest() {
		TestManager.setDefaultProfile(this.getClass());		
	}
	
	@Test
	public void testReport() throws Exception {
		Session session = TestManager.getProfile().getSession();
		TableSchemaFactory factory = new TableSchemaFactory(session);
		TableSchema schema = factory.getSchema("incident");
		assertFalse(schema.isEmpty());
		assertTrue(schema.contains("sys_id"));
		assertTrue(schema.contains("short_description"));
		assertTrue(schema.contains("assignment_group"));
		assertTrue(schema.contains("parent_incident"));
		assertFalse(schema.contains("georgia"));
		schema.report(System.out);
	}

	@Test
	public void testSysTemplate() throws Exception {
		Session session = TestManager.getProfile().getSession();
		TableSchemaFactory factory = new TableSchemaFactory(session);
		TableSchema schema = factory.getSchema("sys_template");
		assertFalse(schema.isEmpty());
		assertTrue(schema.contains("table"));
		assertTrue(schema.contains("show_on_template_bar"));
	}
	
	@Test(expected = InvalidTableNameException.class)
	public void testBadTable() throws Exception {
		Session session = TestManager.getProfile().getSession();
		TableSchemaFactory factory = new TableSchemaFactory(session);
		TableSchema schema = factory.getSchema("incidentxx");
		assertTrue(schema.isEmpty());
	}
		

}
