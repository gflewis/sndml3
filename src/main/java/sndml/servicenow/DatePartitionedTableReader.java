package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatePartitionedTableReader extends TableReader {

	final TableReaderFactory factory;
	final int threads;
	final Interval interval;
	final WriterMetrics writerMetrics = new WriterMetrics();
	
	DateTimeRange range;
	DatePartition partition;
//	List<TableReader> partReaders;
	List<Future<TableReader>> futures;

	final int REST_DEFAULT_PAGE_SIZE = 200;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public DatePartitionedTableReader(TableReaderFactory factory, Interval interval, Integer threads) {
		super(factory.getTable());
		this.factory = factory;
		setQuery(factory.filter);
		setCreatedRange(factory.createdRange);
		setUpdatedRange(factory.updatedRange);
		setFields(factory.fieldNames);
		setPageSize(factory.pageSize);
		this.interval = interval;
		this.threads = (threads == null ? 0 : threads.intValue());
		setWriter(factory.getWriter());
	}

	public int getDefaultPageSize() {
		return REST_DEFAULT_PAGE_SIZE;
	}
	
	public DatePartition getPartition() {
		return this.partition;
	}
		
	public int getThreadCount() {
		return this.threads;
	}

	public int numPartsTotal() {
		assert futures != null;
		return futures.size();
	}
	
	@Override
	public WriterMetrics getWriterMetrics() {
		return this.writer.getMetrics();
	}
	
	public int numPartsComplete() {
		assert futures != null;
		int count = 0;
		for (Future<TableReader> part : futures) {
			count += (part.isDone() ? 1 : 0);
		}
		return count;
	}

	public int numPartsIncomplete() {
		assert futures != null;
		int count = 0;
		for (Future<TableReader> part : futures) {
			count += (part.isDone() ? 0 : 1);
		}
		return count;
	}
	
	@Override
	public void initialize() throws IOException, InterruptedException {
		try {
			super.initialize();
		} catch (SQLException e) {
			// impossible
			throw new AssertionError(e);
		}
		EncodedQuery query = getQuery();
		logger.debug(Log.INIT, String.format("initialize query=\"%s\"", query));
		TableStats stats = table.rest().getStats(query, true);
		setExpected(stats.getCount());
		logger.debug(Log.INIT, String.format("expected=%d", getExpected()));	
		if (getExpected() == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; no readers created");
			return;
		}
		range = stats.getCreated();
		assert range.getStart() != null;
		assert range.getEnd() != null;
		partition = new DatePartition(range, interval);
	}
	
	private TableReader getReader(DateTimeRange partRange) {
		String partName = interval.getName(partRange.getStart());
		TableReader partReader = factory.createReader();
		partReader.setCreatedRange(partRange.intersect(partReader.getCreatedRange()));
		partReader.setReaderName(partReader.getReaderName() + "." + partName);
		partReader.setParent(this);
		return partReader;		
	}

	@Override
	public DatePartitionedTableReader call() throws IOException, SQLException, InterruptedException {
		setLogContext();
		if (getExpected() == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; bypassing call");
			return this;
		}
		futures = new ArrayList<Future<TableReader>>();
		if (threads > 1) {			
			logger.info(Log.INIT, String.format("starting %d threads", threads));			
			ExecutorService executor = Executors.newFixedThreadPool(this.threads);
			for (DateTimeRange partRange : partition) {
				TableReader reader = getReader(partRange);
				logger.debug("Submit " + reader.getReaderName());
				reader.initialize();
				Future<TableReader> future = executor.submit(reader);
				futures.add(future);				
			}
			executor.shutdown();
			while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				logger.info(Log.FINISH, String.format("Waiting for %d / %d partitions to complete", 
						numPartsIncomplete(), numPartsTotal()));
			}
		}
		else {
			for (DateTimeRange partRange : partition) {
				TableReader reader = getReader(partRange);
				reader.initialize();
				reader.call();
			}
		}
		// Free resources
		partition = null;
		return this;
	}

}
