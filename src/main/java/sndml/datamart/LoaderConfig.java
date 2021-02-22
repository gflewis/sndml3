package sndml.datamart;

import java.io.File;
import java.util.List;

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
	public List<JobConfig> tables = new java.util.ArrayList<JobConfig>();

	private static Logger logger = LoggerFactory.getLogger(LoaderConfig.class);
	
	
	public LoaderConfig() {		
	}

	void setMetricsFolder(File metricsFolder) {
		this.metricsFolder = metricsFolder;
		if (metricsFolder != null)
			logger.debug(Log.INIT, "metricsFolder=" + metricsFolder);
	}
	
	File getMetricsFile() {
		return metricsFileName == null ? null : new File(metricsFolder, metricsFileName);
	}

	DateTimeFactory getDateFactory() {
		File metricsFile = getMetricsFile();
		if (metricsFile != null)
			logger.info(Log.INIT, "metricsFile=" + metricsFile);			
		DateTimeFactory dateFactory = 
			(metricsFile != null && metricsFile.canRead()) ?
			dateFactory = new DateTimeFactory(start, metricsFile) :  
			new DateTimeFactory(start);
		logger.info(Log.INIT, String.format("start=%s last=%s", dateFactory.getStart(), dateFactory.getLastStart()));
		return dateFactory;
	}
	
	java.util.List<JobConfig> getJobs() {
		return this.tables;
	}
		
	void updateFields(ConnectionProfile profile) throws ConfigParseException {
		File metricsFile = getMetricsFile();
		DateTimeFactory dateFactory = 
			(metricsFile != null && metricsFile.canRead()) ?
			dateFactory = new DateTimeFactory(start, metricsFile) :  
			new DateTimeFactory(start);
		for (JobConfig table : tables) {
			table.updateFields(profile, dateFactory);
		}
	}
	
	void validate() throws ConfigParseException {
		if (tables.size() < 1) throw new ConfigParseException("No tables");
		for (JobConfig table : tables) {
			table.validateFields();
		}
	}
		
	/*
	@Deprecated
	public LoaderConfig(Table table) throws IOException, ConfigParseException {
		JobConfig config = configFactory.tableLoader(table);
		this.tables.add(config);
	}

	@Deprecated	
	public LoaderConfig(File configFile, Properties props) throws IOException, ConfigParseException {
		this(new FileReader(configFile), props);
	}

	@Deprecated		
	public LoaderConfig(Reader reader, Properties props) throws ConfigParseException {
		File metricsFolder = null;			
		if (props != null) {
			String metricsFolderName = props.getProperty("loader.metrics_folder");
			if (metricsFolderName != null && metricsFolderName.length() > 0)
				metricsFolder = new File(metricsFolderName);
		}		
		JsonNode root = parseYAML(reader);		
		logger.info(Log.INIT, "\n" + root.toPrettyString());
		Iterator<String> fieldnames = root.fieldNames();
		while (fieldnames.hasNext()) {
			String key = fieldnames.next();
		    JsonNode val = root.get(key);
			switch (key.toLowerCase()) {
			case "threads" : 
				threads = val.asInt();
				break;
			case "metrics" :
				String metricsFileName = val.asText();
				metricsFile = (metricsFolder == null) ?
						new File (metricsFileName) : new File(metricsFolder, metricsFileName);
				dateFactory = new DateTimeFactory(start, metricsFile);
				configFactory.setDateFactory(dateFactory);;
				break;
			case "pagesize" : 
				pageSize = val.asInt();
				break;
			case "tables" :
			case "jobs" :
				ArrayNode jobs = (ArrayNode) val;
				for (int i = 0; i < jobs.size(); ++i) {
					JsonNode jobNode = jobs.get(i);
					// JobConfig jobConfig = new JobConfig(this, jobNode);
					JobConfig jobConfig = configFactory.jobConfig(jobNode);
					this.tables.add(jobConfig);
				}
				break;
	    	default:
	    		throw new ConfigParseException("Not recognized: " + key);
			}
		}
		if (tables.size() == 0)
			throw new ConfigParseException("No tables specified");
	}

	public ObjectNode parseYAML(Reader reader) throws ConfigParseException {
		try {
			JsonNode root = yamlMapper.readTree(reader);
			return (ObjectNode) root;
		}
		catch (Exception e) {
			throw new ConfigParseException(e);
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
