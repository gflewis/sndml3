package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public final class DatePartitionedTableReader extends TableReader {

	final JobConfig config;
	final Database db;
	final int threads;
	final Interval interval;
	
	private DateTimeRange range;
	private DatePartition partition;
	private List<Future<Metrics>> futures;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
		
	public DatePartitionedTableReader(Table table, JobConfig config, Database db) {
		super(table);
		this.config = config;
		this.db = db;
		assert config != null;
		if (config.getAction() == Action.SYNC) assert db != null;
		setCreatedRange(config.getCreatedRange(null));
		setUpdatedRange(config.getUpdatedRange());
		this.interval = config.getPartitionInterval();
		this.threads = (config.getThreads()==null) ? 1 : config.getThreads();
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
	public void prepare(RecordWriter writer, Metrics metrics, ProgressLogger progress) 
			throws IOException, InterruptedException {
		super.beginPrepare(writer, metrics, progress);
		assert metrics != null;
		assert progressLogger != null;
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
		super.endPrepare(expected);
	}
	
	private TableReader createReader(DatePart datePart) {
		String partName = datePart.getName();
		TableReader partReader = config.createReader(table, db, datePart);
		String jobName = config.getName();
		String partReaderName = Objects.isNull(partName) ? jobName : jobName + "." + partName;
		assert partReaderName != null;
		Metrics partMetrics = new Metrics(partReaderName, this.metrics);
		partReader.setWriter(writer, partMetrics);		
		ProgressLogger partLogger = progressLogger.newPartLogger(partMetrics, datePart);
		partReader.setProgressLogger(partLogger);
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
		this.logStart();
		if (getExpected() == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; bypassing call");
			return metrics;
		}
		if (threads > 1) {			
			futures = new ArrayList<Future<Metrics>>();
			logger.info(Log.INIT, String.format("starting %d threads", threads));			
			ExecutorService executor = Executors.newFixedThreadPool(this.threads);
			for (DatePart partRange : partition) {
				TableReader partReader = createReader(partRange);
				logger.debug("Submit " + metrics.getName());
				partReader.prepare(writer, metrics, progressLogger);
				Future<Metrics> future = executor.submit(partReader);
				futures.add(future);				
			}
			executor.shutdown();
			while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				logger.info(Log.FINISH, String.format("Waiting for %d / %d partitions to complete", 
						numPartsIncomplete(), numPartsTotal()));
			}
		}
		else {
			for (DatePart partRange : partition) {
				TableReader partReader = createReader(partRange);
				partReader.prepare(writer, metrics, progressLogger);
				assert partReader.getProgressLogger() != null;
//				Metrics metrics = partReader.call();				
//				assert metrics == partReader.getMetrics();
			}
		}
		this.logComplete();
		// Free resources
		futures = null;
		partition = null;
		return metrics;
	}

}
