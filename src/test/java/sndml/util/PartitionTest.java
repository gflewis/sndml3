package sndml.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionTest {

	Logger logger = LoggerFactory.getLogger(this.getClass());

	@Test
	public void testHalfMonth() {
		DateTimeRange range = new DateTimeRange("2024-01-01", "2024-01-15");
		DatePartition partition = new DatePartition(range, IntervalSize.MONTH);
		assertEquals(1, partition.computeSize());
		assertEquals(1, partition.iterator().getSize());
	}

	@Test
	public void testOneMonth() {
		DateTimeRange range = new DateTimeRange("2024-01-01", "2024-01-31");
		DatePartition partition = new DatePartition(range, IntervalSize.MONTH);
		assertEquals(1, partition.computeSize());
		assertEquals(1, partition.iterator().getSize());
	}
	
	@Test
	public void testSixMonths() {
		DateTime start = new DateTime("2024-01-15");
		DateTime end = new DateTime("2024-06-15");
		DateTimeRange range = new DateTimeRange(start, end);
		DatePartition partition = new DatePartition(range, IntervalSize.MONTH);
		DatePartitionIterator iterator = partition.iterator();
		int size = iterator.getSize();
		assertEquals(size, 6);
		DatePart part = null;
		for (int i = 0; i < 6; ++i) {
			part = iterator.next();
			logger.info(Log.TEST, String.format("testSixMonths %s %s", part.getName(), part.getRange().toString()));
			if (i == 0) assertEquals(new DateTime("2024-06-01"), part.getStart());
			if (i == 0) assertEquals(new DateTime("2024-07-01"), part.getEnd());
			if (i == 5) assertEquals(new DateTime("2024-01-01"), part.getStart());
			if (i == 5) assertEquals(new DateTime("2024-02-01"), part.getEnd());
		}
		assertNull(iterator.next());
		assertNull(iterator.next());
	}
	
	@Test
	public void testEmptyRange() {
		DateTime start = new DateTime("2024-06-15 10:30:00");
		DateTime end = new DateTime("2024-06-15 10:30:00");
		DateTimeRange range = new DateTimeRange(start, end);
		DatePartition partition = new DatePartition(range, IntervalSize.HOUR);
		int size = partition.computeSize();
		assertEquals(0, size);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidRange() {
		DateTime start = new DateTime("2024-06-15 10:30:00");
		DateTime end = new DateTime("2024-06-12 10:30:00");
		DateTimeRange range = new DateTimeRange(start, end);
		DatePartition partition = new DatePartition(range, IntervalSize.HOUR);
		int size = partition.computeSize();
		assertEquals(0, size);		
	}
	
	@Test
	public void testEmptyPartition() {
		DatePartition partition = new DatePartition(null, null);
		int size = partition.iterator().getSize();
		assertEquals(size, 0);		
	}
	
}
