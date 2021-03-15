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

import sndml.servicenow.*;

public class Loader {

	final Session session;
	final Database database;
	LoaderConfig config;
	int threads;
	File metricsFile = null;
	PrintWriter statsWriter;
//	WriterMetrics loaderMetrics = new WriterMetrics();	
	
	ArrayList<JobRunner> jobs = new ArrayList<JobRunner>();
	
	static final Logger logger = LoggerFactory.getLogger(Loader.class);
	
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
		String profileName = cmd.getOptionValue("p");
		String yamlFileName = cmd.getOptionValue("y");
		String tableName = cmd.getOptionValue("t");
		boolean isDaemon = cmd.hasOption("d");
		ConnectionProfile profile = new ConnectionProfile(new File(profileName));
		if (isDaemon) {
			if (yamlFileName != null)
				throw new CommandOptionsException("Cannot specify both --daemon and --yaml");
			if (tableName != null)
				throw new CommandOptionsException("Cannot specify both --daemon and --table");
			Daemon daemon = new Daemon(profile);
			daemon.run();			
		}
		else {
			if (yamlFileName != null && tableName != null)
				throw new CommandOptionsException("Cannot specify both --table and --yaml");
			if (yamlFileName == null && tableName == null)
				throw new CommandOptionsException("Must specify --daemon or --yaml or --table");			
//			Session session = profile.getSession();
			ConfigFactory factory = new ConfigFactory();
			if (tableName != null) {
//				Table table = session.table(tableName);
				SimpleTableLoader tableLoader = new SimpleTableLoader(profile, tableName);
				tableLoader.call();
//				config = new LoaderConfig();
//				config.tables.add(factory.tableLoader(profile, table));
			}
			else {
				File yamlFile = new File(yamlFileName);
				String yamlText = readFully(yamlFile);
				logger.info(Log.INIT, yamlFileName + ":\n" + yamlText.trim());
				FileReader reader = new FileReader(new File(yamlFileName));
				LoaderConfig config = factory.loaderConfig(profile, reader);
				Loader loader = new Loader(profile, config);			
				loader.loadTables();			
			}
		}			
	}
			
	Loader(ConnectionProfile profile, LoaderConfig config) {
		this.session = profile.getSession();
		this.database = profile.getDatabase();
		this.config = config;
		this.threads = config.getThreads();
		logger.debug(Log.INIT, String.format("starting loader threads=%d", this.threads));
		this.metricsFile = config.getMetricsFile();
		for (JobConfig jobConfig : config.getJobs()) {
			JobRunner runner = new JobRunner(session, database, jobConfig);
			jobs.add(runner);
		}
	}
	
	@Deprecated
	Session getSession() {
		return session;
	}
	
	@Deprecated
	Database getDatabase() {
		return database;
	}
		
	public WriterMetrics loadTables() throws SQLException, IOException, InterruptedException {
		ArrayList<String> allJobNames = new ArrayList<String>();
		ArrayList<WriterMetrics> allJobMetrics = new ArrayList<WriterMetrics>();
		Log.setGlobalContext();
		WriterMetrics loaderMetrics = new WriterMetrics();			
		loaderMetrics.start();		
		for (JobRunner job : jobs) {
			assert job.getName() != null;
			WriterMetrics jobMetrics = job.call();
			assert jobMetrics != null;
			loaderMetrics.add(jobMetrics);
			allJobNames.add(job.getName());
			allJobMetrics.add(jobMetrics);				
		}
		assert allJobNames.size() == allJobMetrics.size();
		if (metricsFile != null) {
			logger.info(Log.FINISH, "Writing " + metricsFile.getPath());
			statsWriter = new PrintWriter(metricsFile);
			loaderMetrics.write(statsWriter);
			for (WriterMetrics jobMetrics : allJobMetrics) {
				assert jobMetrics.getName() != null;
				jobMetrics.write(statsWriter);				
			}
//			for (int i = 0; i < allJobNames.size(); ++i) {			
//				String jobName = allJobNames.get(i);
//				WriterMetrics jobMetrics = allJobMetrics.get(i);
//				jobMetrics.write(statsWriter, jobName);
//			}
			statsWriter.close();					
		}
		return loaderMetrics;
	}
	
	public WriterMetrics loadTablesConcurrent() 
			throws SQLException, IOException, InterruptedException, ExecutionException {
		ArrayList<Future<WriterMetrics>> futures = new ArrayList<Future<WriterMetrics>>();
		Log.setGlobalContext();
		WriterMetrics loaderMetrics = new WriterMetrics();			
		loaderMetrics.start();
		logger.info(Log.INIT, String.format("starting %d threads", threads));
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		for (JobRunner job : jobs) {
			logger.info(Log.INIT, "submitting " + job.getName());
			Future<WriterMetrics> future = executor.submit(job);
			futures.add(future);
		}
        executor.shutdown();
        while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        		logger.debug(Log.FINISH, "awaiting job completion");
        }	 			
		Log.setGlobalContext();
		loaderMetrics.finish();
		statsWriter = new PrintWriter(metricsFile);
		for (Future<WriterMetrics> future : futures) {
			WriterMetrics jobMetrics = future.get();
			jobMetrics.write(statsWriter);
			
		}
		return loaderMetrics;
	}
//	
//	@Deprecated
//	void writeAllMetrics() throws IOException {
//		logger.info(Log.FINISH, "Writing " + metricsFile.getPath());
//		statsWriter = new PrintWriter(metricsFile);
//		loaderMetrics.write(statsWriter);
//		for (JobRunner job : jobs) {			
//			job.getWriterMetrics().write(statsWriter, job.getName());
//		}
//		statsWriter.close();		
//	}

	public static String readFully(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		StringBuffer text = new StringBuffer();
		while (reader.ready()) text.append(reader.readLine() + "\n");
		reader.close();
		return text.toString();		
	}
		
}
