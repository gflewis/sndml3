package sndml.datamart;

import sndml.servicenow.*;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimestampHashTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTimestampHash() {
		RecordKey key1 = new RecordKey("4715ab62a9fe1981018c3efb96143495");
		RecordKey key2 = new RecordKey("4715ab62a9fe1981018c3efb96143495");
		assertEquals(key1, key2);
		RecordKey key3 = new RecordKey("d71da88ac0a801670061eabfe4b28f77");
		assertNotEquals(key1, key3);
		DateTime d1 = new DateTime("2015-11-02 20:49:08");
		DateTime d2 = new DateTime("2016-03-24 17:47:36");		
		
		TimestampHash h = new TimestampHash();
		assertEquals(0, h.size());
		h.put(key1, d1);	
		assertEquals(1, h.size());
		assertNotNull(h.get(key1));
		assertTrue(h.containsKey(key1));
		assertTrue(h.containsKey(key2));
		assertNotNull(h.get(key2));	
		h.put(key3, d2);
		assertEquals(h.get(key2), d1);
		assertNotNull(h.get(key3));
		assertNotEquals(h.get(key1), h.get(key3));
	}


}
