package sndml.servicenow;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;

import sndml.datamart.DatePart;
import sndml.datamart.DatePartition;
import sndml.datamart.Interval;

public class DatePartitionTest {
	
	Logger logger = TestManager.getLogger(DatePartitionTest.class);

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
	
	@Test
	public void testEvenHour() {
		testRange("testEvenHour", "2020-01-01", "2020-01-02", Interval.HOUR);
	}
	
	@Test
	public void testUnevenHour() {
		testRange("testUnevenHour", "2020-01-01 03:47:15", "2020-01-02 15:19:07", Interval.HOUR);
	}
	
	public int testRange(String name, String start, String end, Interval interval) {
		TestManager.bannerStart(this.getClass(), name);
		DateTime startDate = new DateTime(start);
		DateTime endDate = new DateTime(end);
		DateTimeRange range = new DateTimeRange(startDate, endDate);
		DatePartition partition = new DatePartition(range, interval);
		assertNotNull(partition);
		logger.info(Log.TEST, String.format(
				"Testing: %s", partition.toString()));
		DateTimeRange oldest = null, newest = null;
		int size = 0;
		for (DateTimeRange part : partition) {
			DateTime pstart = part.getStart();
			DateTime pend = part.getEnd();
			assertTrue(pstart.compareTo(pend) < 0);
			assertEquals(pstart.truncate(interval), pstart);
			assertEquals(pend.truncate(interval), pend);
			if (size == 0) newest = part;
			oldest = part;
			size += 1;
			logger.info(Log.TEST, String.format(
				"%d %s %s", size, part.toString(), DatePart.getName(interval,  pstart)));			
		}
		if (size > 0) {
			assertTrue(oldest.getStart().toString().compareTo(start) <= 0);
			assertTrue(newest.getEnd().toString().compareTo(end) >= 0);
		}
		return size;
		
	}
	

}
