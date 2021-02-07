package sndml.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class LoaderConfig {
	
	final DateTime start = DateTime.now();
	private DateTimeFactory dateFactory = new DateTimeFactory(start);
	private ConfigFactory configFactory = new ConfigFactory(start);
	
	private ObjectNode root;
	public Integer threads = 0;
	public Integer pageSize;
	public File metricsFile = null;
	
	public java.util.List<JobConfig> tables = 
			new java.util.ArrayList<JobConfig>();

	private static Logger logger = LoggerFactory.getLogger(LoaderConfig.class);
	
	@Deprecated
	static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
	
	public LoaderConfig() {		
	}

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
		this.root = parseYAML(reader);		
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
	
	java.util.List<JobConfig> getJobs() {
		return this.tables;
	}
	
	String getString(String key) {
		assert root != null;
		assert key != null;
		return root.get(key).asText();
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
