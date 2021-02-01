package sndml.datamart;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

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
		options.addOption(Option.builder("y").longOpt("yaml").required(false).hasArg(true).
				desc("YAML config file (required)").build());
		options.addOption(Option.builder("d").longOpt("daemon").required(false).hasArg(false).
				desc("Run as daemon/service").build());
		CommandLine cmd = new DefaultParser().parse(options,  args);
		String profilename = cmd.getOptionValue("p");
		String yamlfilename = cmd.getOptionValue("y");
		String tablename = cmd.getOptionValue("t");
		boolean isDaemon = cmd.hasOption("d");
		ConnectionProfile profile = new ConnectionProfile(new File(profilename));
		if (isDaemon) {
			if (yamlfilename != null)
				throw new CommandOptionsException("Cannot specify both --daemon and --yaml");
			if (tablename != null)
				throw new CommandOptionsException("Cannot specify both --daemon and --table");
			Daemon daemon = new Daemon(profile);
			daemon.run();
			
		}
		else {
			if (yamlfilename != null && tablename != null)
				throw new CommandOptionsException("Cannot specify both --table and --yaml");
			if (yamlfilename == null && tablename == null)
				throw new CommandOptionsException("Must specify --daemon or --yaml or --table");			
			Session session = profile.getSession();
			LoaderConfig config = (tablename != null) ?
				// Single table load
				new LoaderConfig(session.table(tablename)) :
				// YAML config load
				new LoaderConfig(new File(yamlfilename), profile.getProperties());
			config.validate();
			Loader loader = new Loader(profile, config);			
			loader.loadTables();			
		}
			
	}
	
	Loader(Table table, Database database) {
		this.session = table.getSession();
		this.database = database;
		jobs.add(new LoaderJob(table, database));
	}
		
	Loader(ConnectionProfile profile, LoaderConfig config) {
		this.session = profile.getSession();
		this.database = profile.getDatabase();
		this.config = config;
		this.threads = config.getThreads();
		logger.debug(Log.INIT, String.format("starting loader threads=%d", this.threads));
		this.metricsFile = config.getMetricsFile();
		for (JobConfig jobConfig : config.getJobs()) {
			jobs.add(new LoaderJob(this, jobConfig));
		}
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
