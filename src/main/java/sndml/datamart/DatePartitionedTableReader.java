package sndml.datamart;

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

import sndml.servicenow.*;

public final class DatePartitionedTableReader extends TableReader {

	final TableReaderFactory factory;	
	final int threads;
	final Interval interval;
	
	private DateTimeRange range;
	private DatePartition partition;
	private List<Future<Metrics>> futures;

	final int REST_DEFAULT_PAGE_SIZE = 200;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public DatePartitionedTableReader(
			TableReaderFactory factory, String readerName, 
			Interval interval, Integer threads) {
		super(factory.getTable());
		this.factory = factory;
		this.setReaderName(readerName);
		setFilter(factory.getFilter());
		setCreatedRange(factory.getCreatedRange());
		setUpdatedRange(factory.getUpdatedRange());
		setFields(factory.getFieldNames());
//		setPageSize(factory.getPageSize());
		this.interval = interval;
		this.threads = (threads == null ? 0 : threads.intValue());
		this.metrics = factory.parentWriterMetrics;
		setWriter(factory.getWriter(), metrics);
	}
	
	public int getDefaultPageSize() {
		return REST_DEFAULT_PAGE_SIZE;
	}
	
	public DatePartition getPartition() {
		assert partition != null : "Not initialized";
		return partition;
	}
		
	@SuppressWarnings("unused")
	private int getThreadCount() {
		return this.threads;
	}

	@Override
	public Metrics getMetrics() {
		return this.metrics;
	}

	private int numPartsTotal() {
		assert futures != null;
		return futures.size();
	}
		
	@SuppressWarnings("unused")
	private int numPartsComplete() {
		assert futures != null;
		int count = 0;
		for (Future<Metrics> part : futures) {
			count += (part.isDone() ? 1 : 0);
		}
		return count;
	}

	private int numPartsIncomplete() {
		assert futures != null;
		int count = 0;
		for (Future<Metrics> part : futures) {
			count += (part.isDone() ? 0 : 1);
		}
		return count;
	}
	
	@Override
	public void initialize() throws IOException, InterruptedException {
		super.beginInitialize();
		// Use Stats API to determine min and max dates
		EncodedQuery query = getQuery();
		logger.debug(Log.INIT, String.format("initialize query=\"%s\"", query));
		TableStats stats = table.rest().getStats(query, true);
		Integer expected = stats.getCount();
		logger.debug(Log.INIT, String.format("expected=%d", expected));	
		if (expected == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; no readers created");
			return;
		}
		range = stats.getCreated();
		assert range.getStart() != null;
		assert range.getEnd() != null;
		this.partition = new DatePartition(range, interval);
		logger.debug(Log.INIT, String.format(
				"range=%s partition=%s expected=%d", 
				range.toString(), partition.toString(), expected));
		super.endInitialize(expected);
	}
	
	private TableReader createReader(DatePart partRange) {
		assert this.readerName != null;
		assert factory.getParentReader() == this;
		TableReader partReader = factory.createReader(partRange);
		assert partReader.hasParent();
		assert partReader.getParent() == this;
		partReader.setCreatedRange(partRange.intersect(partReader.getCreatedRange()));
		Metrics partWriterMetrics = partReader.getMetrics();
		ProgressLogger partLogger = progressLogger.newPartLogger(partReader, partRange);
		partReader.setProgressLogger(partLogger);
//		ReaderMetrics partReaderMetrics = partReader.getReaderMetrics();
//		partReaderMetrics.setParent(this.readerMetrics);		
		assert partWriterMetrics.getName() != null;
//		assert partReaderMetrics != this.getReaderMetrics();
		assert partWriterMetrics != this.getMetrics();
		assert partWriterMetrics.hasParent();
		assert partWriterMetrics.getParent() == this.metrics;
		assert partLogger.getReader() == partReader;
		assert partLogger.hasPart();
		assert partWriterMetrics.getName() != metrics.getName();
		return partReader;		
	}

	@Override
	public void logStart() {
		metrics.start();
		super.logStart();		
	}
	
	@Override
	public void logComplete() {
		metrics.finish();
		super.logComplete();
	}
	
	@Override
	public Metrics call() throws IOException, SQLException, InterruptedException {
		logStart();
		if (getExpected() == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; bypassing call");
			return metrics;
		}
		if (threads > 1) {			
			futures = new ArrayList<Future<Metrics>>();
			logger.info(Log.INIT, String.format("starting %d threads", threads));			
			ExecutorService executor = Executors.newFixedThreadPool(this.threads);
			for (DatePart partRange : partition) {
				TableReader reader = createReader(partRange);
				logger.debug("Submit " + reader.getReaderName());
				reader.initialize();
				Future<Metrics> future = executor.submit(reader);
				futures.add(future);				
			}
			executor.shutdown();
			while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				logger.info(Log.FINISH, String.format("Waiting for %d / %d partitions to complete", 
						numPartsIncomplete(), numPartsTotal()));
			}
		}
		else {
			int processed = 0;
			for (DatePart partRange : partition) {
				TableReader reader = createReader(partRange);
				reader.initialize();
				Metrics metrics = reader.call();
				assert metrics == reader.getMetrics();
				processed += reader.getMetrics().getProcessed();
			}
			assert metrics.getProcessed() == processed;
		}
		logComplete();
		// Free resources
		futures = null;
		partition = null;
		return metrics;
	}

}
