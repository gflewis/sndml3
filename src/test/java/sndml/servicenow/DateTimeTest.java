package sndml.servicenow;

import org.junit.*;
import static org.junit.Assert.*;

import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.DatePartition;
import sndml.datamart.Interval;

public class DateTimeTest {

	Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Test
	public void testEqualsDateTime() {
		DateTime d1 = DateTime.now();
		DateTime d2 = DateTime.now();
		assertEquals(0, d1.compareTo(d2));
		assertTrue(d1.equals(d2));
	}

	@Test (expected = InvalidDateTimeException.class)
	public void testBadDate() {
		DateTime d1 = new DateTime("2014-01-15 abcd");
		assertNull(d1);
	}

	@Test (expected = InvalidDateTimeException.class)
	public void testEmptyDate() {
		DateTime d1 = new DateTime("");
		assertNull(d1);
	}


	@Test
	public void testCompareTo() throws InvalidDateTimeException {
		DateTime d1 = new DateTime("2014-01-15 12:00:00");
		DateTime d2 = new DateTime("2014-01-15 12:00:17");
		assertTrue(d2.compareTo(d1) > 0);
		assertTrue(d1.compareTo(d2) < 0);
		assertTrue(d2.compareTo(d2) == 0);
		assertEquals(17, d2.compareTo(d1));
	}

	@Test
	public void testAddSeconds() throws InvalidDateTimeException {
		DateTime d1 = new DateTime("2014-01-15 12:00:00");
		DateTime d2 = new DateTime("2014-01-15 12:00:17");
		assertTrue(d2.equals(d1.addSeconds(17)));
		assertEquals(17, d2.compareTo(d1));
		assertEquals(-17, d1.compareTo(d2));
	}
	
	@Test 
	public void testCompareEqual() {
		DateTime d1 = new DateTime("2014-05-26 15:34:53");
		DateTime d2 = new DateTime("2014-05-26 15:34:53");
		DateTime d3 = new DateTime("2014-05-26");
		DateTime d4 = new DateTime("2014-05-26 00:00:00");
		// DateTime d5 = new DateTime(2014, 5, 26);
		assertEquals(0, d1.compareTo(d2));
		assertEquals(0, d2.compareTo(d1));
		assertTrue(d3.equals(d4));
		assertTrue(d4.equals(d3));
		assertFalse(d3.equals(d2));
		assertTrue(d4.toString().length() > d3.toString().length());
		// assertTrue(d5.equals(d4));
	}
	
	@Test
	public void testTimeZone() throws InvalidDateTimeException {
		@SuppressWarnings("deprecation")
		Date d1 = new Date("Sat May 10 23:58:12 GMT 2014");
		DateTime d2 = new DateTime("2014-05-10 23:58:12");
//		logger.info("d1=" + d1);
//		logger.info("d2=" + d2.toDate().toString());
		assertEquals(d1.toString(), d2.toDate().toString());
	}

	@Test 
	public void testIncrement() throws Exception {
		DateTime d1 = new DateTime("2014-05-26 15:34:53");
		DateTime d2 = new DateTime("2014-12-31 15:34:53");
		assertEquals("2014-05-26 15:34:53", d1.toString());
		assertEquals(2014, d1.getYear());
		assertEquals(5, d1.getMonth());
		assertEquals(new DateTime("2014-06-01"), d1.incrementBy(Interval.MONTH));
		assertEquals(new DateTime("2015-01-01"), d2.incrementBy(Interval.MONTH));		
	}
	
	@Test
	public void testDecrement() throws Exception {
		DateTime d1 = new DateTime("2014-01-01 15:34:53");
		DateTime d2 = new DateTime("2014-01-01 00:00:00");
		DateTime d3 = new DateTime("2013-12-01 00:00:00");
		assertEquals(d3, d1.decrementBy(Interval.MONTH));
		assertEquals(d3, d2.decrementBy(Interval.MONTH));				
	}
	
	@Test
	public void testTruncate() throws Exception {
		DateTime start;
		start = new DateTime("2014-05-10");
		assertEquals(new DateTime("2014-05-01"), start.truncate(Interval.MONTH));
		assertEquals(new DateTime("2014-04-01"), start.truncate(Interval.QUARTER));
		assertEquals(new DateTime("2014-01-01"), start.truncate(Interval.YEAR));	
		start = new DateTime("2016-10-04 17:18:18");
		assertEquals(new DateTime("2016-10-01"), start.truncate(Interval.MONTH));
		assertEquals(new DateTime("2016-10-01"), start.truncate(Interval.QUARTER));
		assertEquals(new DateTime("2016-01-01"), start.truncate(Interval.YEAR));					
	}
	
	@Test
	public void testTruncateWeek() throws Exception {
		// Week should be truncated to Sunday morning
		DateTime start = new DateTime("2021-02-10 14:30:00");
		assertEquals(new DateTime("2021-02-07"), start.truncate(Interval.WEEK));				
	}
	
	@Test
	public void testPartition() throws Exception {
		DateTime start = new DateTime("2014-05-10");
		DateTime end   = new DateTime("2016-12-17");
		DateTimeRange range = new DateTimeRange(start, end);
		DatePartition partition = new DatePartition(range, Interval.MONTH);
		logger.info("partition=" + partition.toString());
		int size = 0;
		DateTimeRange newest = null;
		DateTimeRange oldest = null;
		for (DateTimeRange part : partition) {
			if (size == 0) newest = part;
			oldest = part;
			size += 1;
		}
		assertNotNull(newest);
		assertNotNull(oldest);
		logger.info(Log.TEST, String.format("Size=%d Newest=%s Oldest=%s", 
				size, newest.toString(), oldest.toString()));
		assertTrue(size > 0);
		assertEquals("2014-05-01", oldest.getStart().toString());
		assertEquals("2016-12-01", newest.getStart().toString());
		assertEquals("2017-01-01", newest.getEnd().toString());
	}
	
}
