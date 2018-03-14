package servicenow.datamart;

import servicenow.api.*;

import static org.junit.Assert.*;
import org.junit.*;

import java.io.File;
import java.io.StringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoaderConfigTest {

	Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Test
	public void testSimple() throws Exception {
		String yaml = "tables: [core_company, incident]";
		LoaderConfig config = new LoaderConfig(new StringReader(yaml));
		assertEquals(2, config.getJobs().size());
		assertEquals("core_company", config.getJobs().get(0).getName());
		assertEquals("incident", config.getJobs().get(1).getName());
		assertEquals(false, config.getJobs().get(0).getTruncate());
	}

	@Test
	public void testGoodConfig1() throws Exception {
		File config1 = yamlFile("goodconfig1");
		LoaderConfig config = new LoaderConfig(config1);
		DateTime start = config.getStart();
		DateTime today = DateTime.today();
		assertEquals(8, config.getJobs().size());
		assertEquals("sys_user", config.getJobByName("sys_user").getTargetName());
		assertEquals("rm_story", config.getJobByName("rm_story").getTargetName());		
		assertEquals(new DateTime("2017-01-01"), config.getJobByName("rm_story").getCreated().getStart());
		assertEquals(start, config.getJobByName("rm_story").getCreated().getEnd());
		assertEquals(today, config.getJobByName("cmdb_ci_service").getSince());
	}
	
	@Test(expected=ConfigParseException.class)
	public void testBadDate() throws Exception {
		File badConfig = yamlFile("baddate");
		LoaderConfig config = new LoaderConfig(badConfig);
		TableConfig job = config.getJobByName("incident");
		assertNotNull(job);
		logger.debug(Log.TEST, String.format("name=%s created=%s", job.getName(), job.getCreated()));
		fail();
	}

	@Test(expected=ConfigParseException.class)
	public void testBadInteger() throws Exception {
		File badConfig = yamlFile("badinteger");
		LoaderConfig config = new LoaderConfig(badConfig);
		config.validate();
		fail();
	}

	@Test
	public void testGoodSync1() throws Exception {
		File goodConfig = yamlFile("goodsync1");
		LoaderConfig config = new LoaderConfig(goodConfig);
		config.validate();
	}
	
	@Test(expected=ConfigParseException.class) 
	public void testBadSync1() throws Exception {
		File badConfig = yamlFile("badsync1");
		LoaderConfig config = new LoaderConfig(badConfig);
		config.validate();
		fail();		
	}
	
	static public File yamlFile(String name) {
		assert name != null;
		return new File("src/test/resources/yaml/" + name + ".yaml");
	}
}
