package servicenow.datamart;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.core.*;

public class Loader {

	LoaderConfig config;
	int threads;
	File metricsFile = null;
	PrintWriter statsWriter;
	WriterMetrics loaderMetrics = new WriterMetrics("_loader_");	
	ArrayList<TableLoader> jobs = new ArrayList<TableLoader>();
	
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public static void main(String[] args) throws Exception {
		Log.setGlobalContext();
		Options options = new Options();
		options.addOption(Option.builder("p").longOpt("profile").required(true).hasArg(true).
				desc("Property file (required)").build());
		options.addOption(Option.builder("t").longOpt("table").required(false).hasArg(true).
				desc("Table name").build());
		options.addOption(Option.builder("y").longOpt("config").required(false).hasArg(true).
				desc("Config file (required)").build());
		Globals.initialize(options, args);				
		if (Globals.hasOptionValue("t") && Globals.hasOptionValue("y"))
			throw new CommandOptionsException("Cannot specify both --table and --config");
		Session session = new Session(Globals.getProperties());
		Database datamart = new Database(Globals.getProperties());
		ResourceManager.setSession(session);
		ResourceManager.setDatabase(datamart);
		if (Globals.hasOptionValue("t")) {
			// Single table load
			String tablename = Globals.getOptionValue("t");
			session.validate();
			Table table = session.table(tablename);
			Loader loader = new Loader(table);
			loader.loadTables();
		}
		if (Globals.hasOptionValue("y")) {
			// YAML config file
			File configFile = new File(Globals.getOptionValue("y"));
			LoaderConfig config = new LoaderConfig(configFile);
			Loader loader = new Loader(config);
			loader.loadTables();			
		}
	}
	
	Loader(Table table) {
		jobs.add(new TableLoader(table));
	}
	
	Loader(LoaderConfig config) {
		this.config = config;
		this.threads = config.getThreads();
		logger.debug(String.format("starting loader threads=%d", this.threads));
		this.metricsFile = Globals.getMetricsFile();
		for (TableConfig jobConfig : config.getJobs()) {
			jobs.add(new TableLoader(jobConfig));
		}
	}
	
	public void loadTables() throws SQLException, IOException, InterruptedException {
		Log.setGlobalContext();
		loaderMetrics.start();
		if (threads > 1) {
			logger.info(Log.INIT, String.format("starting %d threads", threads));
			ExecutorService executor = Executors.newFixedThreadPool(threads);
			for (TableLoader job : jobs) {
				logger.info(Log.INIT, "submitting " + job.getName());
				executor.submit(job);
			}
	        executor.shutdown();
	        while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
	        		logger.debug(Log.TERM, "awaiting job completion");
	        }	 			
		}
		else {
			for (TableLoader job : jobs) {
				job.call();
			}			
		}
		Log.setGlobalContext();
		loaderMetrics.finish();
		for (TableLoader job : jobs) loaderMetrics.add(job.getMetrics());
		if (metricsFile != null) writeAllMetrics();
	}
	
	void writeAllMetrics() throws IOException {
		logger.info(Log.TERM, "Writing " + metricsFile.getPath());
		statsWriter = new PrintWriter(metricsFile);
		loaderMetrics.write(statsWriter);
		for (TableLoader job : jobs) {			
			job.getMetrics().write(statsWriter, job.getName());
		}
		statsWriter.close();		
	}

}
