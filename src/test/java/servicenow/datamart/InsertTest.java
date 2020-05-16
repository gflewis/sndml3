package servicenow.datamart;

import servicenow.api.*;

import static org.junit.Assert.*;
import org.junit.*;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.api.Session;
import servicenow.api.TestingManager;

@RunWith(Parameterized.class)
public class InsertTest {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestingManager.allProfiles();
	}
		
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	final TestingProfile profile;
	final Session session;
	final Database database;
	
	public InsertTest(TestingProfile profile) throws Exception {
		TestingManager.setProfile(this.getClass(), profile);
		this.profile = profile;
		this.session = profile.getSession();
		this.database = profile.getDatabase();
	}

	@AfterClass
	public static void clear() throws Exception {
		TestingManager.clearAll();
	}

	@Test
	public void testInsert() throws Exception {
		LoaderConfig config = new LoaderConfig(TestingManager.yamlFile("load_incident_truncate"));
		JobConfig job = config.getJobs().get(0);
		assertTrue(job.getTruncate());
		assertEquals(LoaderAction.INSERT, job.getAction());
		LoaderJob loader = new LoaderJob(job, null);
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
		LoaderConfig config = new LoaderConfig(TestingManager.yamlFile("load_incident_twice"));
		Loader loader = new Loader(config);
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
