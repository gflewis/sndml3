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
		setFilter(config.getFilter(table));
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
		assert writer != null;
		assert metrics != null;
		assert progress != null;
		// Use Stats API to determine min and max dates
		EncodedQuery query = this.getQuery();
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
	
	private TableReader createReader(DatePart datePart) 
			throws IOException, SQLException, InterruptedException {
		String partName = datePart.getName();
		boolean createNewSession = (threads > 1) ? true : false;
		TableReader partReader = config.createReader(table, db, datePart, createNewSession);
		String jobName = config.getName();
		String partReaderName = Objects.isNull(partName) ? jobName : jobName + "." + partName;
		assert partReaderName != null;
		Metrics partMetrics = new Metrics(partReaderName, this.metrics);
		ProgressLogger partLogger = progress.newPartLogger(partMetrics, datePart);
		partReader.prepare(writer, partMetrics, partLogger);
		return partReader;		
	}

	@Override
	public Metrics call() throws IOException, SQLException, InterruptedException {
		progress.logStart();
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
				assert partReader.getProgressLogger() != null;
				partReader.call();				
			}
		}
		progress.logComplete();
		// Free resources
		futures = null;
		partition = null;
		return metrics;
	}

}
