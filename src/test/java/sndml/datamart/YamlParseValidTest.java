package sndml.datamart;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import sndml.servicenow.Log;
import sndml.servicenow.TestManager;
import sndml.servicenow.TestingProfile;

public class YamlParseValidTest {

	final Logger logger = TestManager.getLogger(this.getClass());
	final TestingProfile profile = TestManager.getDefaultProfile();
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPass() throws Exception {		
		TestFolder folder = new TestFolder(this.getClass().getSimpleName());	
		ConfigFactory factory = new ConfigFactory();
		for (YamlFile file : folder.yamlFiles()) {
			logger.info(Log.TEST, "Testing " + file.getPath() + 
					"\n" + TestManager.readFully(file).trim());
			try {
				LoaderConfig config = factory.loaderConfig(profile, file);
				String json = factory.jsonMapper.writeValueAsString(config);
				logger.info(Log.TEST, json);
			} catch (FileNotFoundException e) {
				logger.warn(Log.TEST, e.getMessage());
				e.printStackTrace();
				fail("File not found: " + file.getPath());
				return;
			} catch (ConfigParseException e) {
				logger.warn(Log.TEST, e.getMessage());
				e.printStackTrace();
				fail(e.getMessage());
				return;
			}
		}
	}
	

}
