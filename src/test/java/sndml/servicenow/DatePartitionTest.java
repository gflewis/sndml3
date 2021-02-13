package sndml.servicenow;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

public class DatePartitionTest {
	
	Logger logger = TestManager.getLogger(DatePartitionTest.class);

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSimple() {
		int size = testRange("testSimple", "2019-01-01", "2020-01-01", Interval.MONTH);
		assertEquals(12, size);
	}

	@Test
	public void testUnevenMonth() {
		int size = testRange("testUnevenMonth", "2019-01-07", "2020-01-14", Interval.MONTH);
		assertEquals(13, size);
		
	}
	
	@Test
	public void testEmpty() {
		int size = testRange("testEmpty", "2020-01-01", "2020-01-01", Interval.MONTH);
		assertEquals(0, size);
	}
	
	@Test
	public void testUnevenQuarter() {
		int size = testRange("testUnevenQuarter", "2018-01-06 08:36:00", "2019-01-15 15:34:17", Interval.QUARTER);
		assertEquals(5, size);
	}
	
	@Test
	public void testEvenWeek() {
		int size = testRange("testEvenWeek", "2020-11-01", "2020-11-22", Interval.WEEK);
		assertEquals(3, size);
	}
	
	public int testRange(String name, String start, String end, Interval interval) {
		TestManager.bannerStart(this.getClass(), name);
		DateTimeRange range = new DateTimeRange(new DateTime(start), new DateTime(end));
		DatePartition partition = new DatePartition(range, interval);
		assertNotNull(partition);
		DateTimeRange first = null, last = null;
		int size = 0;
		for (DateTimeRange part : partition) {
			if (size == 0) last = part;
			first = part;
			size += 1;
			logger.info(Log.TEST, String.format("%d %s", size, part.toString()));			
		}
		if (size > 0) {
			assertTrue(first.getStart().equals(new DateTime(start)));
			assertTrue(last.getEnd().equals(new DateTime(end)));			
		}
		return size;
		
	}
	

}
