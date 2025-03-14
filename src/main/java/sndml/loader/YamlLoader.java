package sndml.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.agent.JobCancelledException;
import sndml.util.Log;
import sndml.util.Metrics;
import sndml.util.ResourceException;

public class YamlLoader {

	static ConfigFactory factory = new ConfigFactory();
	final Resources resources;
	YamlLoaderConfig config;
	File metricsFile = null;
	PrintWriter statsWriter;
	
	ArrayList<JobRunner> jobs = new ArrayList<JobRunner>();
	
	static final Logger logger = LoggerFactory.getLogger(YamlLoader.class);

	YamlLoader(Resources resources, File yamlFile) throws ResourceException, SQLException, IOException {
		this(resources, new FileReader(yamlFile));
	}
	
	YamlLoader(Resources resources, FileReader reader) throws ResourceException, SQLException, IOException {
		this(resources, factory.loaderConfig(resources.getProfile(), reader));
	}
	
	YamlLoader(Resources resources, YamlLoaderConfig config) {
		this.resources = resources;
		this.config = config;
		this.metricsFile = config.getMetricsFile();
		for (JobConfig jobConfig : config.getJobs()) {
			JobRunner runner = new JobRunner(resources, jobConfig);
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
	
	public Metrics loadTables() throws SQLException, IOException, InterruptedException, JobCancelledException {
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

	/*
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
	*/

	public static String readFully(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		StringBuffer text = new StringBuffer();
		while (reader.ready()) text.append(reader.readLine() + "\n");
		reader.close();
		return text.toString();		
	}
		
}
