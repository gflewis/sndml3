package sndml.servicenow;

import org.junit.*;
import org.slf4j.Logger;

import sndml.loader.Resources;
import sndml.loader.TestManager;

import static org.junit.Assert.*;

public class SessionVerificationTest {

	Logger logger = TestManager.getLogger(this.getClass());
	
	@Test
	public void testValidate() throws Exception {
		Resources resources = new Resources(TestManager.getDefaultProfile());
		Session session = resources.getReaderSession();
		session.verifyUser();
		Table user = session.table("sys_user");
		TableWSDL wsdl = user.getWSDL();
		int wsdlCount = wsdl.getReadFieldNames().size();
		logger.info("wsdl fields=" + wsdlCount);
		TableSchema schema = resources.getSchemaReader().getSchema(user);
		int schemaCount = schema.getFieldNames().size();
		logger.info("schema fields=" + schemaCount);
		session.verifyUser();
		assertEquals(wsdlCount, schemaCount);
	}

//	@Test
//	public void testAutoVerify() throws Exception {
//		Properties props = new Properties();
//		props.setProperty("servicenow.instance", "dev00000");
//		props.setProperty("servicenow.username", "admin");
//		props.setProperty("servicenow.password", "secret");
//		PropertySet propset = new PropertySet(props, "servicenow");
//		Session session1 = new Session(propset);
//		assertNotNull(session1);
//		props.setProperty("servicenow.verify_session", "true");
//		Session session2 = new Session(propset);
//		assertNull(session2);
//	}
}
