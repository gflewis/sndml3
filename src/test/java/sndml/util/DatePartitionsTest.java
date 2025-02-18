package sndml.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.loader.TestManager;

import java.util.NoSuchElementException;

public class DatePartitionsTest {

	Logger logger = LoggerFactory.getLogger(this.getClass());

	@Test
	public void testSimple() {
		int size = testRange("testSimple", "2019-01-01", "2020-01-01", IntervalSize.MONTH);
		assertEquals(12, size);
	}

	@Test
	public void testUnevenMonth() {
		int size = testRange("testUnevenMonth", "2019-01-07", "2020-01-14", IntervalSize.MONTH);
		assertEquals(13, size);
		
	}
	
	@Test
	public void testEmpty() {
		int size = testRange("testEmpty", "2020-01-01", "2020-01-01", IntervalSize.MONTH);
		assertEquals(0, size);
	}
	
	@Test
	public void testUnevenQuarter() {
		int size = testRange("testUnevenQuarter", "2018-01-06 08:36:00", "2019-01-15 15:34:17", IntervalSize.QUARTER);
		assertEquals(5, size);
	}
	
	@Test
	public void testEvenWeek() {
		int size = testRange("testEvenWeek", "2020-11-01", "2020-11-22", IntervalSize.WEEK);
		assertEquals(3, size);
	}
	
	@Test
	public void testEvenHour() {
		testRange("testEvenHour", "2020-01-01", "2020-01-02", IntervalSize.HOUR);
	}
	
	@Test
	public void testUnevenHour() {
		testRange("testUnevenHour", "2020-01-01 03:47:15", "2020-01-02 15:19:07", IntervalSize.HOUR);
	}
	
	public int testRange(String name, String start, String end, IntervalSize interval) {
		TestManager.bannerStart(this.getClass(), name);
		DateTime startDate = new DateTime(start);
		DateTime endDate = new DateTime(end);
		DateTimeRange range = new DateTimeRange(startDate, endDate);
		DatePartitions partitions = new DatePartitions(range, interval);
		assertNotNull(partitions);
		logger.info(Log.TEST, String.format(
				"Testing: %s", partitions.toString()));
		DateTimeRange oldest = null, newest = null;
		int size = 0;
		for (DateTimeRange part : partitions) {
			DateTime pstart = part.getStart();
			DateTime pend = part.getEnd();
			assertTrue(pstart.compareTo(pend) < 0);
			assertEquals(pstart.truncate(interval), pstart);
			assertEquals(pend.truncate(interval), pend);
			if (size == 0) newest = part;
			oldest = part;
			size += 1;
			logger.info(Log.TEST, String.format(
				"%d %s %s", size, part.toString(), Partition.getName(interval,  pstart)));			
		}
		if (size > 0) {
			assertTrue(oldest.getStart().toString().compareTo(start) <= 0);
			assertTrue(newest.getEnd().toString().compareTo(end) >= 0);
		}
		return size;
		
	}
	
	@Test
	public void testHalfMonth() {
		DateTimeRange range = new DateTimeRange("2024-01-01", "2024-01-15");
		DatePartitions partitions = new DatePartitions(range, IntervalSize.MONTH);
		assertEquals(1, partitions.computeSize());
		assertEquals(1, partitions.iterator().getSize());
	}

	@Test
	public void testOneMonth() {
		DateTimeRange range = new DateTimeRange("2024-01-01", "2024-01-31");
		DatePartitions partitions = new DatePartitions(range, IntervalSize.MONTH);
		assertEquals(1, partitions.computeSize());
		assertEquals(1, partitions.iterator().getSize());
	}
	
	@Test
	public void testSixMonths() {
		DateTime start = new DateTime("2024-01-15");
		DateTime end = new DateTime("2024-06-15");
		DateTimeRange range = new DateTimeRange(start, end);
		DatePartitions partitions = new DatePartitions(range, IntervalSize.MONTH);
		DatePartitionIterator iterator = partitions.iterator();
		int size = iterator.getSize();
		assertEquals(size, 6);
		Partition part = null;
		for (int i = 0; i < 6; ++i) {
			part = iterator.next();
			logger.info(Log.TEST, String.format("testSixMonths %s %s", part.getName(), part.getRange().toString()));
			if (i == 0) assertEquals(new DateTime("2024-06-01"), part.getStart());
			if (i == 0) assertEquals(new DateTime("2024-07-01"), part.getEnd());
			if (i == 5) assertEquals(new DateTime("2024-01-01"), part.getStart());
			if (i == 5) assertEquals(new DateTime("2024-02-01"), part.getEnd());
		}
	}
	
	@Test(expected = NoSuchElementException.class)
	public void testSixMonthsEnd() {
		DateTime start = new DateTime("2024-01-15");
		DateTime end = new DateTime("2024-06-15");
		DateTimeRange range = new DateTimeRange(start, end);
		DatePartitions partitions = new DatePartitions(range, IntervalSize.MONTH);
		DatePartitionIterator iterator = partitions.iterator();
		int size = iterator.getSize();
		assertEquals(size, 6);
		Partition part = null;
		for (int i = 0; i < 7; ++i) {
			part = iterator.next();
			assertNotNull(part);
		}		
	}
	
	@Test
	public void testEmptyRange() {
		DateTime start = new DateTime("2024-06-15 10:30:00");
		DateTime end = new DateTime("2024-06-15 10:30:00");
		DateTimeRange range = new DateTimeRange(start, end);
		DatePartitions partitions = new DatePartitions(range, IntervalSize.HOUR);
		int size = partitions.computeSize();
		assertEquals(0, size);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidRange() {
		DateTime start = new DateTime("2024-06-15 10:30:00");
		DateTime end = new DateTime("2024-06-12 10:30:00");
		DateTimeRange range = new DateTimeRange(start, end);
		DatePartitions partitions = new DatePartitions(range, IntervalSize.HOUR);
		int size = partitions.computeSize();
		assertEquals(0, size);		
	}
	
	@Test
	public void testEmptyPartition() {
		DatePartitions partitions = new DatePartitions(null, null);
		int size = partitions.iterator().getSize();
		assertEquals(size, 0);		
	}
	
	@Test(expected = NoSuchElementException.class)
	public void testEmptyException() {
		DateTime start = new DateTime("2024-06-15 10:30:00");
		DateTime end = new DateTime("2024-06-15 10:30:00");
		DateTimeRange range = new DateTimeRange(start, end);
		DatePartitions partitions = new DatePartitions(range, IntervalSize.HOUR);
		int size = partitions.computeSize();
		assertEquals(0, size);		
		DatePartitionIterator iterator = partitions.iterator();
		// should throw an exception
		@SuppressWarnings("unused")
		Partition part = iterator.next();
	}
	
}
