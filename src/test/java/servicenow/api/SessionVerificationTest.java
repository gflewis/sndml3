package servicenow.api;

import org.junit.*;
import org.slf4j.Logger;

import servicenow.api.Session;
import servicenow.api.Table;
import servicenow.api.TableSchema;

import static org.junit.Assert.*;

public class SessionVerificationTest {

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
		session.verify();
		assertEquals(wsdlCount, schemaCount);
	}

}
