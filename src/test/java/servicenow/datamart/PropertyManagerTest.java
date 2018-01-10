package servicenow.datamart;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.slf4j.Logger;

import servicenow.datamart.Globals;

public class PropertyManagerTest {
	
	static private Logger logger = AllTests.getLogger(PropertyManagerTest.class);

	@Test
	public void testExecute() throws Exception {
		String result = Globals.evaluate("echo hello");
		logger.debug("result=" + result);
		assertEquals("hello", result);
	}
	
	@Test
	public void testLassPass() throws Exception {
		logger.debug("Start of Test");
		String cmd = "/usr/local/bin/lpass show --password servicenow.mit.api-gflewis";
		String result = Globals.evaluate(cmd);
		assertTrue(result.length() > 2);
		logger.debug("result=" + result);
		File profile = new File("mit/mitexplp.profile");
		Globals.loadPropertyFile(profile);
		logger.debug("password=" + Globals.getProperties().getProperty("servicenow.password"));
		logger.debug("End of Test");
	}
	

}
