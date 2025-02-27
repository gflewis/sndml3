package sndml.loader;

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

import sndml.agent.JobCancelledException;
import sndml.servicenow.*;
import sndml.util.DatePartition;
import sndml.util.DatePartitionSet;
import sndml.util.DateTimeRange;
import sndml.util.PartitionInterval;
import sndml.util.Log;
import sndml.util.Metrics;
import sndml.util.ProgressLogger;

public final class DatePartitionedTableReader extends TableReader {

	final JobConfig config;
	final DatabaseWrapper db;
	final int threads;
	final PartitionInterval interval;
	
	private DateTimeRange range;
	private DatePartitionSet parts;
	private List<Future<Metrics>> futures;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
		
	public DatePartitionedTableReader(Table table, JobConfig config, DatabaseWrapper db) {
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
		
	public DatePartitionSet getPartitions() {
		assert parts != null : "Not initialized";
		return parts;
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
		if (futures == null) return 0;
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
			range = null;
			return;
		}
		else {
			range = stats.getCreated();
			assert range.getStart() != null : "range.start is null";
			assert range.getEnd() != null : "range.end is null";
		}
		this.parts = new DatePartitionSet(range, interval);
		if (range == null) 
			logger.info(Log.INIT, "expected=0; empty partition created");
		else 
			logger.debug(Log.INIT, String.format(
				"range=%s partition=%s expected=%d", range, parts, expected));
		super.endPrepare(expected);
	}
	
	private TableReader createReader(DatePartition datePart) 
			throws IOException, SQLException, InterruptedException, JobCancelledException {
		String partName = datePart.getName();
		boolean createNewSession = (threads > 1) ? true : false;
		Session mySession = createNewSession ? table.getSession().duplicate() : table.getSession();
		Table myTable = createNewSession ? mySession.table(table.getName()) : table;
		 
		TableReader partReader = config.createReader(myTable, db, mySession, datePart);
		String jobName = config.getName();
		String partReaderName = Objects.isNull(partName) ? jobName : jobName + "." + partName;
		assert partReaderName != null;
		Metrics partMetrics = new Metrics(partReaderName, this.metrics);
		ProgressLogger partLogger = progress.newPartLogger(partMetrics, datePart);
		partReader.prepare(writer, partMetrics, partLogger);
		return partReader;		
	}

	@Override
	public Metrics call() throws IOException, SQLException, InterruptedException, JobCancelledException {
		progress.logStart();
		if (getExpected() == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; bypassing call");
			progress.logComplete();
			return metrics;
		}
		if (threads > 1) {			
			futures = new ArrayList<Future<Metrics>>();
			logger.info(Log.INIT, String.format("starting %d threads", threads));			
			ExecutorService executor = Executors.newFixedThreadPool(this.threads);
			for (DatePartition partRange : parts) {
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
			for (DatePartition partRange : parts) {
				TableReader partReader = createReader(partRange);
				assert partReader.getProgressLogger() != null;
				partReader.call();				
			}
		}
		progress.logComplete();
		// Free resources
		futures = null;
		parts = null;
		return metrics;
	}

}
