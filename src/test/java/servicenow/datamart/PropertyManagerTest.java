package servicenow.datamart;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.slf4j.Logger;

import servicenow.datamart.PropertyManager;

public class PropertyManagerTest {
	
	static private Logger logger = AllTests.getLogger(PropertyManagerTest.class);

	@Test
	public void testExecute() throws Exception {
		String result = PropertyManager.evaluate("echo hello");
		logger.debug("result=" + result);
		assertEquals("hello", result);
	}
	
	@Test
	public void testLassPass() throws Exception {
		logger.debug("Start of Test");
		String cmd = "/usr/local/bin/lpass show --password servicenow.mit.api-gflewis";
		String result = PropertyManager.evaluate(cmd);
		assertTrue(result.length() > 2);
		logger.debug("result=" + result);
		File profile = new File("mit/mitexplp.profile");
		PropertyManager.loadPropertyFile(profile);
		logger.debug("password=" + PropertyManager.getProperties().getProperty("servicenow.password"));
		logger.debug("End of Test");
	}
	

}
