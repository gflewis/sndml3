package sndml.datamart;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

import sndml.servicenow.*;

@RunWith(Parameterized.class)
public class RowCountExceptionTest {

	final TestingProfile profile;
	final TestFolder folder = new TestFolder(this.getClass());
	final Logger logger = TestManager.getLogger(this.getClass());
			
	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestManager.getDatamartProfiles();
	}

	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}
	
	public RowCountExceptionTest(TestingProfile profile) {
		TestManager.setProfile(this.getClass(), profile);
		this.profile = profile;
	}
	
	@Test(expected = TooManyRowsException.class)
	public void testTooManyRows() throws Exception {
		YamlFile yamlFile = folder.getYaml("too-many-rows");	
		TestManager.bannerStart(this.getClass(), "testTooManyRows", yamlFile);
		Loader loader = yamlFile.getLoader(profile);
		JobRunner job = loader.getJob("incident_load");
		assertNotNull(job);
		assertNotNull(job.getConfig());
		Integer maxRows = job.getConfig().getMaxRows();
		assertNotNull(maxRows);
		assertTrue(maxRows < 500);
		loader.loadTables();			
		fail("Exception not detected");
	}
	
	@Test(expected = TooFewRowsException.class)
	public void testTooFewRows() throws Exception {
		YamlFile yamlFile = folder.getYaml("too-few-rows");	
		TestManager.bannerStart(this.getClass(), "testTooFewRows", yamlFile);
		Loader loader = yamlFile.getLoader(profile);
		JobRunner job = loader.getJob("user_load");
		assertNotNull(job);
		assertNotNull(job.getConfig());
		Integer minRows = job.getConfig().getMinRows();
		assertNotNull(minRows);
		assertTrue(minRows > 0);
		loader.loadTables();
		fail("Exception not detected");
	}

}
