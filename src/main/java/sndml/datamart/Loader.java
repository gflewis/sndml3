package sndml.datamart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.daemon.AgentDaemon;
import sndml.servicenow.*;

public class Loader {

	final Session session;
	final Database database;
	LoaderConfig config;
	File metricsFile = null;
	PrintWriter statsWriter;
	
	ArrayList<JobRunner> jobs = new ArrayList<JobRunner>();
	
	static final Logger logger = LoggerFactory.getLogger(Loader.class);
	
	/**
	 * This is the main class invoked from the JAR file.
	 * 
	 */
	public static void main(String[] args) throws Exception {
		Log.setGlobalContext();
		Options options = new Options();
		options.addOption(Option.builder("p").longOpt("profile").required(true).hasArg(true).
				desc("Property file (required)").build());
		options.addOption(Option.builder("t").longOpt("table").required(false).hasArg(true).
				desc("Table name").build());
		options.addOption(Option.builder("y").longOpt("yaml").required(false).hasArg(true).
				desc("YAML config file (required)").build());
		options.addOption(Option.builder("daemon").longOpt("daemon").required(false).hasArg(false).
				desc("Run as daemon/service").build());
		options.addOption(Option.builder("scan").longOpt("scan").required(false).hasArg(false).
				desc("Run the deamon scanner once").build());
		CommandLine cmd = new DefaultParser().parse(options,  args);
		String profileName = cmd.getOptionValue("p");
		ConnectionProfile profile = new ConnectionProfile(new File(profileName));
		int cmdCount = 0;
		if (cmd.hasOption("y")) cmdCount += 1;
		if (cmd.hasOption("t")) cmdCount += 1;
		if (cmd.hasOption("daemon")) cmdCount += 1;
		if (cmd.hasOption("scan")) cmdCount += 1;		
		if (cmdCount != 1) 
			throw new CommandOptionsException(
				"Must specify exactly one of: --yaml, --table, --daemon or --scan");
		if (cmd.hasOption("t")) {
			// Simple Table Loader
			String tableName = cmd.getOptionValue("t");
			Database database = profile.getDatabase();
			SimpleTableLoader tableLoader = new SimpleTableLoader(profile, tableName, database);
			tableLoader.call();
		}
		else if (cmd.hasOption("y")) {
			// YAML file
			String yamlFileName = cmd.getOptionValue("y");
			File yamlFile = new File(yamlFileName);
			String yamlText = readFully(yamlFile);
			logger.info(Log.INIT, yamlFileName + ":\n" + yamlText.trim());
			FileReader reader = new FileReader(new File(yamlFileName));
			ConfigFactory factory = new ConfigFactory();
			LoaderConfig config = factory.loaderConfig(profile, reader);
			Loader loader = new Loader(profile, config);			
			loader.loadTables();
		}
		else if (cmd.hasOption("daemon")) {
			// Daemon
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Starting daemon: " + AgentDaemon.getAgentName());
			daemon.runForever();
		}
		else if (cmd.hasOption("scan")) {
			// Scan once
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Scanning agent: " + AgentDaemon.getAgentName());
			daemon.scanOnce();
		}
	}
				
	Loader(ConnectionProfile profile, LoaderConfig config) throws ResourceException, SQLException {
		this.session = profile.getSession();
		this.database = profile.getDatabase();
		this.config = config;
		this.metricsFile = config.getMetricsFile();
		for (JobConfig jobConfig : config.getJobs()) {
			JobRunner runner = new JobRunner(session, database, jobConfig);
			jobs.add(runner);
		}
	}
	
	JobRunner getJob(int index) {
		return jobs.get(index);
	}
	
	JobRunner getJob(String name) {
		for (JobRunner job : jobs) {
			if (name.equals(job.getConfig().getName()))
				return job;
		}
		return null;
	}
	
	public Metrics loadTables() throws SQLException, IOException, InterruptedException {
		ArrayList<Metrics> allJobMetrics = new ArrayList<Metrics>();
		Log.setGlobalContext();
		Metrics loaderMetrics = new Metrics(null);
		loaderMetrics.start();		
		for (JobRunner job : jobs) {
			assert job.getName() != null;
			Metrics jobMetrics = job.call();
			assert jobMetrics != null;
			loaderMetrics.add(jobMetrics);
			allJobMetrics.add(jobMetrics);				
		}
		if (metricsFile != null) {
			logger.info(Log.FINISH, "Writing " + metricsFile.getPath());
			statsWriter = new PrintWriter(metricsFile);
			loaderMetrics.write(statsWriter);
			for (Metrics jobMetrics : allJobMetrics) {
				assert jobMetrics.getName() != null;
				jobMetrics.write(statsWriter);				
			}
			statsWriter.close();					
		}
		return loaderMetrics;
	}

	@Deprecated
	public Metrics loadTablesConcurrent() 
			throws SQLException, IOException, InterruptedException, ExecutionException {
		int threads = config.getThreads();
		ArrayList<Future<Metrics>> futures = new ArrayList<Future<Metrics>>();
		Log.setGlobalContext();
		Metrics loaderMetrics = new Metrics(null);			
		loaderMetrics.start();
		logger.info(Log.INIT, String.format("starting %d threads", threads));
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		for (JobRunner job : jobs) {
			logger.info(Log.INIT, "submitting " + job.getName());
			Future<Metrics> future = executor.submit(job);
			futures.add(future);
		}
        executor.shutdown();
        while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        		logger.debug(Log.FINISH, "awaiting job completion");
        }	 			
		Log.setGlobalContext();
		for (Future<Metrics> future : futures) {
			Metrics jobMetrics = future.get();
			loaderMetrics.add(jobMetrics);
		}
		loaderMetrics.finish();
		statsWriter = new PrintWriter(metricsFile);
		for (Future<Metrics> future : futures) {
			Metrics jobMetrics = future.get();
			jobMetrics.write(statsWriter);
			
		}
		return loaderMetrics;
	}

	public static String readFully(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		StringBuffer text = new StringBuffer();
		while (reader.ready()) text.append(reader.readLine() + "\n");
		reader.close();
		return text.toString();		
	}
		
}
