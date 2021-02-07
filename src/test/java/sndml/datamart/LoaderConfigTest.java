package sndml.datamart;

import sndml.datamart.LoaderConfig;
import sndml.servicenow.*;

import static org.junit.Assert.*;
import org.junit.*;

import java.io.File;
import java.io.StringReader;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LoaderConfigTest {

	final Logger logger = TestManager.getLogger(this.getClass());
	final TestFolder folder = new TestFolder("yaml");

	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}
	
	@Test
	public void testSimple() throws Exception {
		String yaml = "tables: [core_company, incident]";
		LoaderConfig config = new LoaderConfig(new StringReader(yaml), null);
		assertEquals(2, config.getJobs().size());
		assertEquals("core_company", config.getJobs().get(0).getName());
		assertEquals("incident", config.getJobs().get(1).getName());
		assertEquals(false, config.getJobs().get(0).getTruncate());
	}

	@Test
	public void testGoodConfig1() throws Exception {
		File config1 = folder.getYaml("goodconfig1");
		LoaderConfig config = new LoaderConfig(config1, null);
		DateTime start = config.getStart();
		DateTime today = DateTime.today();
		assertEquals(8, config.getJobs().size());
		assertEquals("sys_user", config.getJobByName("sys_user").getTarget());
		assertEquals("rm_story", config.getJobByName("rm_story").getTarget());		
		assertEquals(new DateTime("2017-01-01"), config.getJobByName("rm_story").getCreated().getStart());
		assertEquals(start, config.getJobByName("rm_story").getCreated().getEnd());
		assertEquals(today, config.getJobByName("cmdb_ci_service").getSince());
	}
	
	@Test
	public void testGoodSync1() throws Exception {
		File goodConfig = folder.getYaml("goodsync1");
		LoaderConfig config = new LoaderConfig(goodConfig, null);
		config.validate();
	}
	
	@Test
	public void test_createIncident() throws JacksonException {
		ConfigFactory factory = new ConfigFactory();
		ObjectNode obj = TestManager.yaml("{action: create, source: incident, drop: true}");
		JobConfig config = factory.jobConfig(obj);
		assertEquals(JobAction.CREATE, config.getAction());
		assertFalse(config.getTruncate());
		assertTrue(config.dropTable);
	}
	
		
}
