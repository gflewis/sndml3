package servicenow.datamart;

import servicenow.api.*;

import static org.junit.Assert.*;
import org.junit.*;

import java.io.File;
import java.io.StringReader;
import org.slf4j.Logger;

public class LoaderConfigTest {

	final Logger logger = TestingManager.getLogger(this.getClass());
	final TestFolder folder = new TestFolder("yaml");

	@AfterClass
	public static void clear() throws Exception {
		TestingManager.clearAll();
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
		assertEquals("sys_user", config.getJobByName("sys_user").getTargetName());
		assertEquals("rm_story", config.getJobByName("rm_story").getTargetName());		
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
		
}
