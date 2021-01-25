package sndml.datamart;

import static org.junit.Assert.*;

import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;
import org.junit.Test;
import org.slf4j.Logger;

import sndml.datamart.ConfigParseException;
import sndml.datamart.LoaderConfig;
import sndml.datamart.YamlFile;
import sndml.servicenow.Log;
import sndml.servicenow.TestingManager;

@RunWith(Parameterized.class)
public class ConfigParseExceptionTest {

	public YamlFile yamlFile;
	static final TestFolder folder = new TestFolder("ConfigParseError");
	Logger logger = TestingManager.getLogger(this.getClass());
	
	
	@Parameters(name = "{index}:{0}")
	public static YamlFile[] files() {
		return folder.yamlFiles();
	}
	
	public ConfigParseExceptionTest(YamlFile file) {
		this.yamlFile = file;
	}
	
	@Test
	public void test() throws Exception {
		TestingManager.bannerStart(this.getClass(), "test", yamlFile);
		LoaderConfig config = null;
		Throwable err = null;
		try {
			config = yamlFile.getConfig(null);
			config.validate();
		}
		catch (ConfigParseException e) {
			err = e;
			logger.warn(Log.TEST, e.getMessage());
		}
		assertNotNull(err);
		logger.warn(Log.TEST, err.getMessage());		
		assertTrue(err instanceof ConfigParseException);
	}

}
