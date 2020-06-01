package servicenow.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.api.*;

public class LoaderConfig extends Config {

	final DateTime start = DateTime.now();
	
	private Map root;
	private Integer threads = 0;
	private Integer pageSize;
	private File metricsFile = null;
	
	private final java.util.List<JobConfig> tables = 
			new java.util.ArrayList<JobConfig>();

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	public LoaderConfig(Table table) throws IOException, ConfigParseException {
		this.tables.add(new JobConfig(table));
	}
	
	public LoaderConfig(File configFile, Properties props) throws IOException, ConfigParseException {
		this(new FileReader(configFile), props);
	}
		
	public LoaderConfig(Reader reader, Properties props) throws ConfigParseException {
		File metricsFolder = null;				
		if (props != null) {
			String metricsFolderName = props.getProperty("loader.metrics_folder");
			if (metricsFolderName != null && metricsFolderName.length() > 0)
				metricsFolder = new File(metricsFolderName);
		}		
		this.root = parseDocument(reader);		
		logger.info(Log.INIT, "\n" + parser.dump(root).trim());
		for (String key : root.keySet()) {
		    Object val = root.get(key);
			switch (key.toLowerCase()) {
			case "threads" : 
				threads = asInteger(val); 
				break;
			case "metrics" :
				String metricsFileName = val.toString();
				metricsFile = (metricsFolder == null) ?
						new File (metricsFileName) : new File(metricsFolder, metricsFileName);
				break;
			case "pagesize" : 
				pageSize = asInteger(val);
				break;
			case "tables" :
			case "jobs" :
				for (Object job : toList(val)) {
					this.tables.add(new JobConfig(this, job));
				}
				break;
	    	default:
	    		throw new ConfigParseException("Not recognized: " + key);
			}
		}
		if (tables.size() == 0)
			throw new ConfigParseException("No tables specified");
	}
	
	String getString(String key) {
		assert root != null;
		return root.getString(key);
	}
		
	java.util.List<JobConfig> getJobs() {
		return this.tables;
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
		
	File getMetricsFile() {
		return this.metricsFile;
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

	void validate() throws ConfigParseException {
		for (JobConfig job : tables) 
			job.validate();
	}
}
