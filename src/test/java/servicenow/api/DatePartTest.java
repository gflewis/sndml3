package servicenow.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatePartTest {

	final private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Test
	public void testDatePart() throws Exception {
		TestingManager.loadProfile("mydev");
		Session session = TestingManager.getSession();
		Table table = session.table("incident");
		RecordListAccumulator accumulator = new RecordListAccumulator(table);
		TableStats stats = table.rest().getStats(null, true);
		TableReaderFactory factory = new RestTableReaderFactory(table, accumulator);
		String parentName = "my_parent_reader";
		factory.setReaderName(parentName);
		logger.info(Log.TEST, "range=" + stats.getCreated().toString());
		DateTime.Interval interval = DateTime.Interval.MONTH;
		PartSumTableReader parentReader = new PartSumTableReader(factory, interval, 0);
		DatePartition partition;
		parentReader.initialize();
		partition = parentReader.getPartition();
		assertNotNull(partition);
		List<TableReader> partReaders = parentReader.getReaders();
		logger.info(Log.TEST, String.format("readers=%d", partReaders.size()));
		String childName = partReaders.get(0).getReaderName();
		logger.info(Log.TEST, partReaders.get(0).getReaderName());
		assertTrue(childName.startsWith(parentName + ".M"));		
	}

}
