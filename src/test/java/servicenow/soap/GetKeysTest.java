package servicenow.soap;

import org.junit.*;

import servicenow.core.*;

import static org.junit.Assert.*;

import java.io.IOException;

public class GetKeysTest {

   static Session session;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		session = TestingManager.getSession();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testAllKeys() throws IOException {
		Table inc = session.table("cmn_department");
		KeyList keys = inc.getKeys();
		assertTrue("keys.size() must be greater than 0", keys.size() > 0);
	}

}
