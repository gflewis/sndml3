package sndml.loader;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertySetTest {

	static private Logger logger = LoggerFactory.getLogger(PropertySetTest.class);

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSampleFile() throws Exception {
		File file = new File("src/test/resources/profile_test.properties");
		ConnectionProfile profile = new ConnectionProfile(file);
		int size = profile.reader.size();
		logger.debug("size=" + size);		
		assertEquals("dev00000", profile.getProperty("reader.instance"));
		assertEquals("dev00000", profile.reader.getProperty("instance"));
		assertEquals("admin", profile.database.getProperty("username"));
		assertEquals("200", profile.getProperty("reader.pagesize"));
		assertEquals("200", profile.reader.getProperty("pagesize"));
		assertEquals("200", profile.app.getProperty("pagesize"));
	}
		
}
