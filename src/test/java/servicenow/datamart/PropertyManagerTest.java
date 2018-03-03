package servicenow.datamart;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyManagerTest {
	
	static private Logger logger = LoggerFactory.getLogger(PropertyManagerTest.class);

	@Test
	public void testExecute() throws Exception {
		String result = Globals.evaluate("echo Lorem ipsum dolor");
		logger.debug("result=" + result);
		assertEquals("Lorem ipsum dolor", result);
	}
	
	@Test @Ignore
	public void testLassPass() throws Exception {
		logger.debug("Start of Test");
		File profile = new File("profiles/lptest.profile");
		Globals.loadPropertyFile(profile);
		String proppw = Globals.getProperties().getProperty("servicenow.password");
		assertNotNull(proppw);
		String lpkey = Globals.getProperties().getProperty("lastpass.key");
		assertNotNull(lpkey);
		String cmd = "/usr/local/bin/lpass show --password " + lpkey;
		String lppw = Globals.evaluate(cmd);
		assertTrue(lppw.length() > 2);
		logger.debug("result=" + lppw);
		logger.debug("password=" + Globals.getProperties().getProperty("servicenow.password"));
		logger.debug("End of Test");
	}
	

}
