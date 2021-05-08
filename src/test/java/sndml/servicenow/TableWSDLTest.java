package sndml.servicenow;

import org.junit.*;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;

import static org.junit.Assert.*;

public class TableWSDLTest {

	final Logger logger = TestManager.getLogger(TableWSDLTest.class);
	final Session session;

	public TableWSDLTest() {
		session = TestManager.getDefaultProfile().getSession();
	}
	
	TableWSDL getWSDL(String tablename) throws IOException {
		Table table = session.table(tablename);
		TableWSDL wsdl = table.getWSDL();
		return wsdl;
	}
	
	@Test
	public void testGoodTable() throws Exception {
		String tablename = "incident";
		TableWSDL wsdl = getWSDL(tablename);
		List<String> columns = wsdl.getReadFieldNames();
		int count = columns.size();
		logger.info(tablename + " has " + count + " columns");
		assert(count > 60);
	}

	@Test (expected = InvalidTableNameException.class)
	public void testBadTableName() throws Exception {
		String tablename = "incidentxxx";
		@SuppressWarnings("unused")
		TableWSDL wsdl = getWSDL(tablename);
		fail();
	}

	@Test
	public void testDefaultWSDL() throws Exception {
		String tablename = "incident";
		TableWSDL wsdl = getWSDL(tablename);
		assertTrue(wsdl.canReadField("sys_updated_on"));
		assertFalse(wsdl.canReadField("dv_assigned_to"));
		assertFalse(wsdl.canReadField("createdxxxxx"));
		assertTrue(wsdl.canWriteField("short_description"));
		assertFalse(wsdl.canWriteField("short_descriptionxxx"));
	}

	@Test
	public void testDisplayValues() throws Exception {
		String tablename = "incident";
		TableWSDL wsdl = new TableWSDL(session, tablename, true);
		assertTrue(wsdl.canReadField("sys_updated_on"));
		assertTrue(wsdl.canReadField("dv_assigned_to"));
		assertFalse(wsdl.canReadField("createdxxxxx"));
		assertTrue(wsdl.canWriteField("short_description"));
		assertFalse(wsdl.canWriteField("short_descriptionxxx"));
	}

	@Test
	public void testDisplayValues2() throws Exception {
		String tablename = "incident";
		TableWSDL wsdl = new TableWSDL(session, tablename, true);
		assertTrue(wsdl.canReadField("sys_updated_on"));
		assertTrue(wsdl.canReadField("dv_assigned_to"));
		assertFalse(wsdl.canReadField("createdxxxxx"));
	}
	
	@Test
	public void testSysTemplate() throws Exception {
		String tablename = "sys_template";
		TableWSDL wsdl = new TableWSDL(session, tablename, true);
		assertTrue(wsdl.canReadField("sys_created_on"));
		assertTrue(wsdl.canReadField("sys_updated_on"));
		assertTrue(wsdl.canReadField("short_description"));
	}
	
}
