package servicenow.datamart;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import servicenow.api.TestingManager;
import servicenow.api.TestingProfile;
import sndml.datamart.Loader;
import sndml.datamart.YamlFile;
import sndml.servicenow.WriterMetrics;

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
		TestingManager.setProfile(this.getClass(), profile);		
	}
	
	@Parameters(name = "{index}:{0}")
	public static TestParam[] getParams() {
		TestingProfile[] allProfiles = TestingManager.allProfiles();
		YamlFile[] allYamlFiles = new TestFolder("Loads").yamlFiles();
		int size = allProfiles.length * allYamlFiles.length;
		TestParam[] result = new TestParam[size];
		int index = 0;
		for (TestingProfile profile : allProfiles) {
			for (YamlFile yamlFile : allYamlFiles) {
				result[index++] = new TestParam(profile, yamlFile);
			}
		}
		return result;
	}
		
	@After
	public void closeProfile() {
		profile.close();
	}

	@Test
	public void test() throws Exception {
		Loader loader = yamlFile.getLoader(profile);
		WriterMetrics metrics = loader.loadTables();
		int processed = metrics.getProcessed();
		assertTrue(processed > 0);
	}

}
