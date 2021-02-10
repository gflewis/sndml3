package sndml.datamart;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import sndml.servicenow.Log;
import sndml.servicenow.TestManager;

public class YamlParsePassTest {

	final Logger logger = TestManager.getLogger(this.getClass());
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPass() throws Exception {		
		TestFolder folder = new TestFolder("yaml_parse/pass");
		ConfigFactory factory = new ConfigFactory();
		for (YamlFile file : folder.yamlFiles()) {
			logger.info(Log.TEST, "Testing " + file.getPath() + 
					"\n" + TestManager.readFully(file).trim());
			try {
				LoaderConfig config = factory.loaderConfig(file);
				String json = factory.jsonMapper.writeValueAsString(config);
				logger.info(Log.TEST, json);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				fail("File not found: " + file.getPath());
				return;
			} catch (ConfigParseException e) {
				e.printStackTrace();
				fail(e.getMessage());
				return;
			}
		}
	}
	

}
