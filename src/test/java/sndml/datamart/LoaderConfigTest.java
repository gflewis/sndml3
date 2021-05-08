package sndml.datamart;

import sndml.servicenow.*;

import static org.junit.Assert.*;
import org.junit.*;

import java.io.File;
import java.io.StringReader;
import org.slf4j.Logger;

public class LoaderConfigTest {

	final Logger logger = TestManager.getLogger(this.getClass());
	final TestingProfile profile = TestManager.getDefaultProfile();
	final TestFolder folder = new TestFolder(this.getClass().getSimpleName());		
	final ConfigFactory factory = new ConfigFactory();


	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}
	
	@Test
	@Ignore
	// This syntax is no longer supported
	public void testSimpleOld() throws Exception {
		String yaml = "tables: [core_company, incident]";
		LoaderConfig config = factory.loaderConfig(profile, new StringReader(yaml));
		assertEquals(2, config.getJobs().size());
		assertEquals("core_company", config.getJobs().get(0).getName());
		assertEquals("incident", config.getJobs().get(1).getName());
		assertEquals(false, config.getJobs().get(0).getTruncate());
	}

	public void testSimpleNew() throws Exception {
		String yaml = "tables: [{source: core_company}, {source: incident}]";
		LoaderConfig config = factory.loaderConfig(profile, new StringReader(yaml));
		assertEquals(2, config.getJobs().size());
		assertEquals("core_company", config.getJobs().get(0).getName());
		assertEquals("incident", config.getJobs().get(1).getName());
		assertEquals(false, config.getJobs().get(0).getTruncate());
	}
	
	@Test
	public void testGoodConfig1() throws Exception {
		File config1 = folder.getYaml("multi-table-load");
		LoaderConfig config = factory.loaderConfig(profile, config1);
		DateTime start = config.getStart();
		DateTime today = DateTime.today();
		assertEquals(8, config.getJobs().size());
		assertEquals("sys_user", config.getJobByName("sys_user").getTarget());
		assertEquals("rm_story", config.getJobByName("rm_story").getTarget());		
		assertEquals(new DateTime("2017-01-01"), config.getJobByName("rm_story").getCreatedRange(null).getStart());
		assertEquals(start, config.getJobByName("rm_story").getCreatedRange(null).getEnd());
		assertEquals(today, config.getJobByName("cmdb_ci_service").getSince());
	}
	
	@Test
	public void testGoodSync1() throws Exception {
		File goodConfig = folder.getYaml("incident-sync");
		LoaderConfig config = factory.loaderConfig(profile, goodConfig);
		assertNotNull(config);
	}
	
	/*
	@Test
	public void test_createIncident() throws JacksonException {
		ObjectNode obj = TestManager.yaml("{action: create, source: incident, drop: true}");
		JobConfig config = factory.jobConfig(obj);
		assertEquals(JobAction.CREATE, config.getAction());
		assertFalse(config.getTruncate());
		assertTrue(config.dropTable);
	}
	*/
	
		
}
