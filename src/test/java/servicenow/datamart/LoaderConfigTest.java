package servicenow.datamart;

import static org.junit.Assert.*;

import java.io.File;
import java.io.StringReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.core.DateTime;
import servicenow.datamart.ConfigParseException;
import servicenow.datamart.LoaderConfig;

public class LoaderConfigTest {

	Logger logger = LoggerFactory.getLogger(this.getClass());
	
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
	public void testConfig1() throws Exception {
		File config1 = new File("src/test/resources/config1.yaml");
		LoaderConfig config = new LoaderConfig(config1);
		assertEquals(8, config.getJobs().size());
		assertEquals("sys_user", config.getJobs().get(0).getName());
		assertEquals("rm_story", config.getJobs().get(4).getName());		
		assertEquals(new DateTime("2017-01-01"), config.getJobs().get(4).getCreated().getStart());
		assertEquals(null, config.getJobs().get(4).getCreated().getEnd());
		assertEquals(DateTime.now(), config.getJobs().get(5).getCreated().getEnd());
		assertEquals(DateTime.today(), config.getJobs().get(7).getUpdated().getStart());
	}
	
	@Test(expected = ConfigParseException.class)
	public void testBadDate() throws Exception {
		File badConfig = new File("src/test/resources/baddate.yaml");
		@SuppressWarnings("unused")
		LoaderConfig config = new LoaderConfig(badConfig);
		fail();
	}

	@Test(expected = ConfigParseException.class)
	public void testBadInteger() throws Exception {
		File badConfig = new File("src/test/resources/badinteger.yaml");
		@SuppressWarnings("unused")
		LoaderConfig config = new LoaderConfig(badConfig);
		fail();
	}

}
