package sndml.datamart;

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
	final ConfigFactory factory = new ConfigFactory();
	TestFolder folder = new TestFolder(this.getClass().getSimpleName());	
	
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
		YamlFile yaml = folder.getYaml("load_truncate");
		TestManager.bannerStart(this.getClass(), "testInsert", profile, yaml);
		LoaderConfig config = factory.loaderConfig(profile, yaml);
		JobConfig job = config.getJobs().get(0);
		assertTrue(job.getTruncate());
		assertEquals(Action.INSERT, job.getAction());
		JobRunner loader = new TestJobRunner(profile, job);
		loader.call();
		WriterMetrics metrics = loader.getWriterMetrics();
		int processed = metrics.getProcessed();
		assertTrue(processed > 0);
		assertEquals(processed, metrics.getInserted());
		assertEquals(0, metrics.getUpdated());
		assertEquals(0, metrics.getDeleted());
		assertEquals(0, metrics.getSkipped());
	}

	@Test
	public void testInsertTwice() throws Exception {
		YamlFile yaml = folder.getYaml("load_twice");
		TestManager.bannerStart(this.getClass(), "testInsertTwice", profile, yaml);
		LoaderConfig config = factory.loaderConfig(profile, yaml);
		Loader loader = new Loader(profile, config);
		// job[0] is droptable
		JobRunner job1 = loader.jobs.get(1);
		JobRunner job2 = loader.jobs.get(2);
		loader.loadTables();
		int rows = job1.getWriterMetrics().getProcessed();
		logger.info(Log.TEST, String.format("rows=%d", rows));
		assertTrue(rows > 0);
		WriterMetrics metrics2 = job2.getWriterMetrics();
		int processed = metrics2.getProcessed();
		assertEquals(rows, processed);
		assertEquals(0, metrics2.getInserted());
		assertEquals(0, metrics2.getUpdated());
		assertEquals(0, metrics2.getDeleted());
		assertEquals(rows, metrics2.getSkipped());
	}
	
}
