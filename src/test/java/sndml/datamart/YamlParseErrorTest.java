package sndml.datamart;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import sndml.servicenow.Log;
import sndml.servicenow.TestManager;
import sndml.servicenow.TestingProfile;

public class YamlParseErrorTest {

	final Logger logger = TestManager.getLogger(this.getClass());
	TestingProfile profile = TestManager.getDefaultProfile();
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testFail() throws IOException {
		TestFolder folder = new TestFolder(this.getClass().getSimpleName());
		ConfigFactory factory = new ConfigFactory();
		for (YamlFile file : folder.yamlFiles()) {
			String text = TestManager.readFully(file);
			logger.info(Log.TEST, "Testing " + file.getPath() + "\n" + text);
			Throwable err = null;			
			try {
				@SuppressWarnings("unused")
				LoaderConfig config = factory.loaderConfig(profile, file);
				fail("No exception for " + file.getPath());				
			} catch (ConfigParseException e) {
				err = e;
				logger.info(Log.TEST, e.getMessage());
			}
			assertNotNull(err);
			logger.warn(Log.TEST, err.getMessage());		
			assertTrue(err instanceof ConfigParseException);			
		}
	}

}
