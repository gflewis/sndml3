package servicenow.rest;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.core.*;
import servicenow.rest.MultiDatePartReader;

public class DatePartTest {

	final private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Test
	public void testDatePart() throws Exception {
		TestingManager.loadProfile("mitexp");
		Session session = TestingManager.getSession();
		Table table = session.table("incident");
		RecordListAccumulator accumulator = new RecordListAccumulator(table);
		TableStats stats = table.rest().getStats(null, true);
		logger.info(Log.TEST, "range=" + stats.getCreated().toString());
		DateTime.Interval interval = DateTime.Interval.MONTH;
		MultiDatePartReader reader = new MultiDatePartReader(table.rest(), interval, EncodedQuery.all(), null, null, 0, accumulator);
		reader.initialize();
	}

}
