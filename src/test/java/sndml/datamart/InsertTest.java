package sndml.datamart;

import sndml.servicenow.*;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

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
	Properties metrics;
		
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

	@Test
	public void testInsert() throws Exception {
		YamlFile yaml = folder.getYaml("load_truncate");
		TestManager.bannerStart(this.getClass(), "testInsert", profile, yaml);
		LoaderConfig config = factory.loaderConfig(profile, yaml);
		JobConfig job = config.getJobs().get(0);
		assertTrue(job.getTruncate());
		assertEquals(Action.INSERT, job.getAction());
		JobRunner loader = new TestJobRunner(profile, job);
		Metrics metrics = loader.call();
		int processed = metrics.getProcessed();
		assertTrue(processed > 0);
		assertEquals(processed, metrics.getInserted());
		assertEquals(0, metrics.getUpdated());
		assertEquals(0, metrics.getDeleted());
		assertEquals(0, metrics.getSkipped());
	}

	private void loadMetrics(File file) throws FileNotFoundException, IOException {
		metrics = new Properties();
		metrics.load(new FileReader(file));
	}
	
	private int getMetric(String name) {
		String propValue = metrics.getProperty(name);
		if (propValue == null) return -9999;
		return Integer.parseInt(propValue);
	}
	
	@Test
	public void testInsertTwice() throws Exception {
		YamlFile yaml = folder.getYaml("load_twice");
		File metricsFile = new File("/tmp/load_twice.metrics");
		TestManager.bannerStart(this.getClass(), "testInsertTwice", profile, yaml);
		LoaderConfig config = factory.loaderConfig(profile, yaml);
		Loader loader = new Loader(profile, config);
		loader.loadTables();
		loadMetrics(metricsFile);
		int rows = getMetric("load1.processed");
		logger.info(Log.TEST, String.format("rows=%d", rows));
		assertTrue(rows > 0);
		assertEquals(getMetric("load1.processed"), getMetric("load2.processed"));
		assertEquals(0, getMetric("load2.inserted"));
		assertEquals(0, getMetric("load2.updated"));
		assertEquals(0, getMetric("load2.deleted"));
		assertEquals(rows, getMetric("load2.skipped"));
	}
	
}
