package sndml.datamart;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Test that values enclosed in backtics are evaluated
 */
public class EvaluateTest {
	
	static private Logger logger = LoggerFactory.getLogger(EvaluateTest.class);

	@Test
	public void testExecute() throws Exception {
		String result = ConnectionProfile.evaluate("echo Lorem ipsum dolor");
		logger.debug("result=" + result);
		assertEquals("Lorem ipsum dolor", result);
	}
	
	@Test
	public void testSampleFile() throws Exception {
		File file = new File("src/test/resources/profile_test.properties");
		ConnectionProfile profile = new ConnectionProfile(file);
		assertEquals("orange", profile.getProperty("servicenow.password"));
		assertEquals("yellow", profile.getProperty("datamart.password"));
		
	}
	
	/*
	@Test
	public void testSysProperties() throws Exception {
		File file = new File("src/test/resources/profile_test.properties");
		ConnectionProfile profile = new ConnectionProfile(file);
		String username = profile.getProperty("servicenow.username");
		String password = profile.getProperty("servicenow.password");
		assertNotNull(username);
		assertNotNull(password);
		assertNotNull(System.getProperty("sndml.servicenow.instance"));
		assertNotNull(System.getProperty("sndml.servicenow.username"));
		assertEquals(username, System.getProperty("sndml.servicenow.username"));
		assertNull(System.getProperty("sndml.servicenow.password"));		
	}
	*/
	
}
