package servicenow.core;

import org.junit.AfterClass;

import servicenow.core.KeyList;
import servicenow.core.Session;
import servicenow.core.Table;

public class KeyListTest {

	static Session session;
	static Table tbl;
	static KeyList keys;

	// TODO: implement
	/*
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		session = AllTests.getSession();
		tbl = session.table("sys_user_group");
		keys = tbl.getKeys();
		tbl.setChunkSize(30);
	}

	@Test
	public void testReadTable() throws Exception {
		RecordList recs2 = tbl.getRecords(keys, 0, keys.size());
		assertEquals(recs2.size(), keys.size());
		RecordList recs1 = tbl.reader().getAllRecords();
		assertEquals(recs2.size(), recs1.size());
	}

	@Test
	public void testGetSubset() throws Exception {
		RecordList recs = tbl.getRecords(keys, 0, 15);
		assertEquals(recs.size(), 15);		
	}

	@Test
	public void testGetBigKeys() throws Exception {
		Table big = session.table("cmdb_ci_ip_address");
		BasicTableReader reader = big.reader();
		KeyList keys = reader.getKeys();
		assumeTrue(keys.size() > 40000);
		HashMap<Key,Boolean> hash = new HashMap<Key,Boolean>();
		for (Key key : keys) {
			assertNull(hash.get(key));
			assertTrue(key.isValidGUID());
			hash.put(key, new Boolean(true));
		}
		assertEquals(keys.size(), hash.size());
	}
*/
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

}
