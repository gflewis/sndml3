package servicenow.soap;

import org.junit.*;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;

import servicenow.core.*;
import servicenow.datamart.AllTests;
import servicenow.soap.TableWSDL;

import static org.junit.Assert.*;

public class TableWSDLTest {

	static Logger logger = AllTests.getLogger(TableWSDLTest.class);

	Session session;
	String tablename;
	Table table;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	TableWSDL getWSDL(String tablename) throws IOException {
		this.tablename = tablename;
		this.table = session.table(tablename);
		TableWSDL wsdl = this.table.getWSDL();
		return wsdl;
	}
	
	@Before
	public void setUp() throws IOException {
		session = TestingManager.getSession();
	}

	@Test 
	public void testGoodTable() throws Exception {
		TableWSDL wsdl = getWSDL("incident");
		List<String> columns = wsdl.getReadFieldNames();
		int count = columns.size();
		logger.info(tablename + " has " + count + " columns");
		assert(count > 60);
	}

	@Test (expected = InvalidTableNameException.class)
	public void badTableTest() throws Exception {
		String tablename = "incidentxxx";
		@SuppressWarnings("unused")
		TableWSDL wsdl = getWSDL(tablename);
		fail();
	}

	@Test 
	public void testDefaultWSDL() throws Exception {
		TableWSDL wsdl = getWSDL("incident");
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

	
}
