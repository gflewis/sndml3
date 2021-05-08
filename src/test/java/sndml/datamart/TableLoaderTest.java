package sndml.datamart;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import sndml.servicenow.TestManager;
import sndml.servicenow.TestingProfile;
import sndml.servicenow.Metrics;

@RunWith(Parameterized.class)
public class TableLoaderTest {

	static class TestParam {
		final TestingProfile profile;
		final YamlFile yamlFile;
		TestParam(TestingProfile p, YamlFile f) {
			profile = p;
			yamlFile = f;
		}
		@Override
		public String toString() {
			return profile.toString() + ":" + yamlFile.toString();
		}
	}
		
	final TestingProfile profile;
	final YamlFile yamlFile;
	
	public TableLoaderTest(TestParam param) {
		this.profile = param.profile;
		this.yamlFile = new YamlFile(param.yamlFile);
		TestManager.setProfile(this.getClass(), profile);		
	}
	
	@Parameters(name = "{index}:{0}")	
	public static TestParam[] getParams() {
		TestingProfile[] allProfiles = TestManager.allProfiles();
		TestFolder folder = new TestFolder("TableLoaderTest");	
		int size = folder.listFiles().length;
		TestParam[] result = new TestParam[size];
		int index = 0;
		for (TestingProfile profile : allProfiles) {
			for (YamlFile yamlFile : folder.yamlFiles()) {
				TestManager.bannerStart(TableLoaderTest.class, "Test", profile, yamlFile);
				result[index++] = new TestParam(profile, yamlFile);
			}
		}
		return result;
	}

	@Test
	public void test() throws Exception {
		Loader loader = yamlFile.getLoader(profile);
		Metrics metrics = loader.loadTables();
		int processed = metrics.getProcessed();
		assertTrue(processed > 0);
	}

}
