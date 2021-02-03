package sndml.datamart;

import sndml.datamart.JobConfig;
import sndml.datamart.Loader;
import sndml.datamart.JobAction;
import sndml.datamart.LoaderConfig;
import sndml.datamart.LoaderJob;
import sndml.datamart.YamlFile;
import sndml.servicenow.*;

import static org.junit.Assert.*;
import org.junit.*;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

@RunWith(Parameterized.class)
public class InsertTest {

	final TestingProfile profile;
	final Logger logger = TestManager.getLogger(this.getClass());
	// final TestFolder folder = new TestFolder("yaml");
	
	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestManager.allProfiles();
	}
			
	public InsertTest(TestingProfile profile) throws Exception {
		TestManager.setProfile(this.getClass(), profile);
		this.profile = profile;
	}

	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}

	@After
	public void closeProfile() {
		profile.close();
	}
			
	@Test
	public void testInsert() throws Exception {
		YamlFile yaml = new TestFolder("yaml").getYaml("load_incident_truncate");		
		LoaderConfig config = new LoaderConfig(yaml, null);
		JobConfig job = config.getJobs().get(0);
		assertTrue(job.getTruncate());
		assertEquals(JobAction.LOAD, job.getAction());
		LoaderJob loader = new LoaderJob(profile, job);
		loader.call();
		WriterMetrics metrics = loader.getMetrics();
		int processed = metrics.getProcessed();
		assertTrue(processed > 0);
		assertEquals(processed, metrics.getInserted());
		assertEquals(0, metrics.getUpdated());
		assertEquals(0, metrics.getDeleted());
		assertEquals(0, metrics.getSkipped());
	}

	@Test
	public void testInsertTwice() throws Exception {
		YamlFile yaml = new TestFolder("yaml").getYaml("load_incident_twice");
		LoaderConfig config = new LoaderConfig(yaml, null);
		Loader loader = new Loader(profile, config);
		LoaderJob job1 = loader.jobs.get(0);
		LoaderJob job2 = loader.jobs.get(1);
		loader.loadTables();
		int rows = job1.getMetrics().getProcessed();
		assertTrue(rows > 0);
		WriterMetrics metrics2 = job2.getMetrics();
		int processed = metrics2.getProcessed();
		assertEquals(rows, processed);
		assertEquals(0, metrics2.getInserted());
		assertEquals(0, metrics2.getUpdated());
		assertEquals(0, metrics2.getDeleted());
		assertEquals(rows, metrics2.getSkipped());
	}
	
}
