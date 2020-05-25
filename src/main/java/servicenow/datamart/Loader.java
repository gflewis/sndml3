package servicenow.datamart;

import servicenow.api.*;

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

public class Loader {

	final Session session;
	final Database database;
	LoaderConfig config;
	int threads;
	File metricsFile = null;
	PrintWriter statsWriter;
	WriterMetrics loaderMetrics = new WriterMetrics();	
	ArrayList<LoaderJob> jobs = new ArrayList<LoaderJob>();
	
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
		// ResourceManager.setSession(session);
		// ResourceManager.setDatabase(datamart);
		// TODO Document property session_verify
		if (Globals.getPropertyBoolean("session_verify",  false)) session.verify();
		if (Globals.hasOptionValue("t")) {
			// Single table load
			String tablename = Globals.getOptionValue("t");
			Table table = session.table(tablename);
			Loader loader = new Loader(table, datamart);
			loader.loadTables();
		}
		if (Globals.hasOptionValue("y")) {
			File configFile = new File(Globals.getOptionValue("y"));
			LoaderConfig config = new LoaderConfig(configFile);
			Loader loader = new Loader(session, datamart, config);
			loader.loadTables();			
		}
	}
	
	Loader(Table table, Database database) {
		this.session = table.getSession();
		this.database = database;
		jobs.add(new LoaderJob(table, database));
	}
	
	Loader(Session session, Database database, LoaderConfig config) {
		this.session = session;
		this.database = database;
		this.config = config;
		this.threads = config.getThreads();
		logger.debug(Log.INIT, String.format("starting loader threads=%d", this.threads));
		this.metricsFile = Globals.getMetricsFile();
		for (JobConfig jobConfig : config.getJobs()) {
			jobs.add(new LoaderJob(this, jobConfig));
		}
	}
	
	Loader(ConnectionProfile profile, LoaderConfig config) {
		this(profile.getSession(), profile.getDatabase(), config);		
	}
	
	Session getSession() {
		return session;
	}
	
	Database getDatabase() {
		return database;
	}
	
	WriterMetrics getMetrics() {
		return loaderMetrics;
	}
	
	LoaderJob lastJob() {
		return jobs.get(jobs.size() - 1);
	}
	
	public WriterMetrics loadTables() throws SQLException, IOException, InterruptedException {
		Log.setGlobalContext();
		loaderMetrics.start();
		if (threads > 1) {
			logger.info(Log.INIT, String.format("starting %d threads", threads));
			ExecutorService executor = Executors.newFixedThreadPool(threads);
			for (LoaderJob job : jobs) {
				logger.info(Log.INIT, "submitting " + job.getName());
				executor.submit(job);
			}
	        executor.shutdown();
	        while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
	        		logger.debug(Log.FINISH, "awaiting job completion");
	        }	 			
		}
		else {
			for (LoaderJob job : jobs) {
				job.call();
			}			
		}
		Log.setGlobalContext();
		loaderMetrics.finish();
		if (metricsFile != null) writeAllMetrics();
		return loaderMetrics;
	}
	
	void writeAllMetrics() throws IOException {
		logger.info(Log.FINISH, "Writing " + metricsFile.getPath());
		statsWriter = new PrintWriter(metricsFile);
		loaderMetrics.write(statsWriter);
		for (LoaderJob job : jobs) {			
			job.getMetrics().write(statsWriter, job.getName());
		}
		statsWriter.close();		
	}

}
