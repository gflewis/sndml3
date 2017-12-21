package servicenow.core;

import org.junit.*;
import org.slf4j.Logger;

import servicenow.core.Session;
import servicenow.core.Table;
import servicenow.core.TableSchema;
import servicenow.soap.TableWSDL;

import static org.junit.Assert.*;

public class SessionValidateTest {

	Logger logger = TestingManager.getLogger(this.getClass());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testValidate() throws Exception {
		TestingManager.loadDefaultProfile();
		Session session = TestingManager.getSession();
		Table user = session.table("sys_user");
		TableWSDL wsdl = user.getWSDL();
		int wsdlCount = wsdl.getReadFieldNames().size();
		logger.info("wsdl fields=" + wsdlCount);
		TableSchema schema = user.getSchema();
		int schemaCount = schema.getFieldNames().size();
		logger.info("schema fields=" + schemaCount);
		session.validate();
		assertEquals(wsdlCount, schemaCount);
	}

}
