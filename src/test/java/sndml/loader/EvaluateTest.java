package sndml.loader;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.loader.ConnectionProfile;

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
		assertEquals("orange", profile.reader.getProperty("password"));
		assertEquals("yellow", profile.database.getProperty("password"));
		
	}
	
	
}
