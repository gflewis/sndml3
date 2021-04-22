package sndml.datamart;

import java.io.File;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class LoaderConfig {
	
	@JsonIgnore final DateTime start = DateTime.now();
	@JsonIgnore File metricsFolder = null;
	
	@JsonProperty("threads") public Integer threads;
	@JsonProperty("pagesize") public Integer pageSize;
	@JsonProperty("metrics") public String metricsFileName = null;
	
	@JsonProperty("tables")
	public ArrayList<JobConfig> tables; // = new java.util.ArrayList<JobConfig>();

	private Logger logger = LoggerFactory.getLogger(LoaderConfig.class);
	
	
	public LoaderConfig() {		
	}

	void setMetricsFolder(File metricsFolder) {
		this.metricsFolder = metricsFolder;
		if (metricsFolder != null)
			logger.debug(Log.INIT, "metricsFolder=" + metricsFolder);
	}
	
	File getMetricsFile() {
		if (metricsFileName == null) return null;
		if (metricsFolder == null) return new File(metricsFileName);
		if (metricsFileName.startsWith("/")) return new File(metricsFileName);
		return new File(metricsFolder, metricsFileName);
	}
	
	java.util.List<JobConfig> getJobs() {
		return this.tables;
	}
		
	void updateFields(ConnectionProfile profile) throws ConfigParseException {
		File metricsFile = getMetricsFile();
		DateCalculator dateFactory = 
			(metricsFile != null && metricsFile.canRead()) ?
			dateFactory = new DateCalculator(start, metricsFile) :  
			new DateCalculator(start);
		for (JobConfig table : tables) {
			table.initialize(profile, dateFactory);
		}
	}
	
	void validate() throws ConfigParseException {
		if (tables.size() < 1) throw new ConfigParseException("No tables");
		for (JobConfig table : tables) {
			table.validate();
		}
	}		
	
	/*
	 * Used for JUnit tests
	 */
	JobConfig getJobByName(String name) {
		assert name != null;
		for (JobConfig job : tables) {
			if (name.equals(job.getName())) return job;
		}
		return null;
	}
	
	int getThreads() {
		return this.threads==null ? 0 : this.threads.intValue();
	}
		
	Integer getPageSize() {
		return pageSize;
	}
	
	/**
	 * Return the DateTime that this object was initialized.
	 */
	DateTime getStart() {
		return start;
	}

}
