package sndml.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import sndml.servicenow.DateTime;
import sndml.servicenow.Log;
import sndml.servicenow.Table;

public class ConfigFactory {
	
	ObjectMapper jsonMapper;
	ObjectMapper yamlMapper;
	Logger logger = LoggerFactory.getLogger(ConfigFactory.class);	
	
	ConfigFactory() {
		this(DateTime.now());
	}
	
	ConfigFactory(DateTime start) {
		jsonMapper = new ObjectMapper();
		jsonMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
		jsonMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);
		jsonMapper.setSerializationInclusion(Include.NON_NULL);		
		yamlMapper = new ObjectMapper(new YAMLFactory());
		yamlMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);		
		yamlMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);	
		yamlMapper.setSerializationInclusion(Include.NON_NULL);		
	}

	LoaderConfig loaderConfig(File file) throws IOException, ConfigParseException {
		return loaderConfig(file, null);
	}
	
	LoaderConfig loaderConfig(File file, Properties props) throws IOException, ConfigParseException {
		Reader reader = new FileReader(file);
		return loaderConfig(reader, props);		
	}
	
	LoaderConfig loaderConfig(Reader reader) {
		return loaderConfig(reader);
	}
	
	LoaderConfig loaderConfig(Reader reader, Properties props) 
			throws IOException, ConfigParseException {
		File metricsFolder = null;			
		if (props != null) {
			String metricsFolderName = props.getProperty("loader.metrics_folder");
			if (metricsFolderName != null && metricsFolderName.length() > 0)
				metricsFolder = new File(metricsFolderName);
		}		
		LoaderConfig loader = yamlMapper.readValue(reader, LoaderConfig.class);
		loader.setMetricsFolder(metricsFolder);
		DateTimeFactory dateFactory = loader.getDateFactory();
		// logger.info(Log.INIT, "loaderConfig last=" + dateFactory.getLastStart());
		logger.info(Log.INIT, "loaderConfig: " + jsonMapper.writeValueAsString(loader));
		for (JobConfig job : loader.tables) {
			job.updateFields(dateFactory);
			job.validate();
			logger.info(Log.INIT, job.getName() + ": " + job.toString());
		}
		return loader;
	}

	JobConfig jobConfig(JsonNode node) throws ConfigParseException {
		DateTimeFactory dateFactory = new DateTimeFactory();
		JobConfig job;
		try {
			job = jsonMapper.treeToValue(node, JobConfig.class);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e.getMessage());
		}
		job.updateFields(dateFactory);
		job.validate();
		logger.info(Log.INIT, "jobConfig: " + job.toString());
		return job;
	}
		
	JobConfig tableLoader(Table table) throws ConfigParseException {
		DateTimeFactory dateFactory = new DateTimeFactory();
		JobConfig job = new JobConfig();
		job.source = table.getName();
		job.updateFields(dateFactory);
		job.validate();
		logger.info(Log.INIT, "tableLoader: " + job.toString());
		return job;
	}

	/*
	static private void updateFields(JobConfig job, DateTimeFactory dateFactory) {
		// Determine Start
		if (job.start == null) {
			job.start = dateFactory.getStart();
		}
		// Determine Action
		if (job.action == null)	job.action = 
				Boolean.TRUE.equals(job.truncate) ?	
				JobAction.LOAD : JobAction.REFRESH;
		if (job.action == JobAction.INSERT) job.action = JobAction.LOAD;
		if (job.action == JobAction.UPDATE) job.action = JobAction.REFRESH;
		JobAction action = job.action;
		// Determine Source, Target and Name
		if (job.source == null)	job.source = 
				job.name != null ? job.name : job.target;
		if (job.name == null) job.name = 
				job.target != null ? job.target : job.source;
		if (job.target == null)	job.target = 
				job.source != null ? job.source : job.name;
		
		if (job.sinceValue == null) {
			job.sinceDate = null;
		}
		else {
			job.sinceDate = dateFactory.getDate(job.sinceValue);
		}
		
		if (job.createdValue == null) {
			job.createdRange = null;
		}
		if (job.createdValue != null) {
			job.setDateFactory(dateFactory);
			job.setCreated(job.createdValue);
		}
		
		if (config.pageSize == null && config.parent() != null) {
			config.pageSize = config.parent.getPageSize();
		}
		
		if (action == null) configError("Action not specified");
		if (job.getSource() == null) configError("Source not specified");
		if (job.getTarget() == null) configError("Target not specified");
		if (job.getName() == null) configError("Name not specified");
		booleanValidForActions("Truncate", job.truncate, action, 
				EnumSet.of(JobAction.LOAD));
		booleanValidForActions("Drop", job.dropTable, action, 
				EnumSet.of(JobAction.CREATE));
		validForActions("Created", job.createdRange, action, 
				EnumSet.range(JobAction.LOAD, JobAction.SYNC));
		validForActions("Since", job.sinceDate, action, 
				EnumSet.range(JobAction.LOAD, JobAction.REFRESH));
		if (job.getIncludeColumns() != null && job.getExcludeColumns() != null) 
			configError("Cannot specify both Columns and Exclude");		
	}

	static void booleanValidForActions(String name, Boolean value, JobAction action, EnumSet<JobAction> validActions) {
		if (Boolean.TRUE.equals(value)) {
			if (!validActions.contains(action))
				notValid(name, action);
		}
	}
	
	static void validForActions(String name, Object value, JobAction action, EnumSet<JobAction> validActions)
			throws ConfigParseException {
		if (value != null) {
			if (!validActions.contains(action))
				notValid(name, action);
		}		
	}
		
	static void notValid(String option, JobAction action) throws ConfigParseException {
		String msg = option + " not valid with Action: " + action.toString();
		configError(msg);
	}
	
	*/

	static void configError(String msg) {
		throw new ConfigParseException(msg);
	}	
	
	/*
	LoaderConfig loaderConfig(Reader reader, Properties props) throws ConfigParseException {
		LoaderConfig config = yamlMapper.readValue(reader, LoaderConfig.class);
		
		
		LoaderConfig config = new LoaderConfig();
		DateTime start = DateTime.now();
		File metricsFolder = null;
		JsonNode root;
		if (props != null) {
			String metricsFolderName = props.getProperty("loader.metrics_folder");
			if (metricsFolderName != null && metricsFolderName.length() > 0)
				metricsFolder = new File(metricsFolderName);
		}		
		try {
			root = this.yamlMapper.readTree(reader);
		} catch (IOException e) {
			throw new ConfigParseException(e);
		}
		logger.info(Log.INIT, "\n" + root.toPrettyString());
		Iterator<String> fieldnames = root.fieldNames();
		while (fieldnames.hasNext()) {
			String key = fieldnames.next();
		    JsonNode val = root.get(key);
			switch (key.toLowerCase()) {
			case "threads" : 
				config.threads = val.asInt();
				break;
			case "metrics" :
				String metricsFileName = val.asText();
				File metricsFile = (metricsFolder == null) ?
						new File (metricsFileName) : new File(metricsFolder, metricsFileName);
				dateFactory = new DateTimeFactory(start, metricsFile);
				break;
			case "tables" :
			case "jobs" :
				ArrayNode jobs = (ArrayNode) val;
				for (int i = 0; i < jobs.size(); ++i) {
					JsonNode jobNode = jobs.get(i);
					// JobConfig jobConfig = new JobConfig(this, jobNode);
					JobConfig jobConfig = this.jobConfig(jobNode);
					config.tables.add(jobConfig);
				}
				break;
	    	default:
	    		throw new ConfigParseException("Not recognized: " + key);
			}
		}
		if (config.tables.size() == 0)
			throw new ConfigParseException("No tables specified");
		return config;
	}	
	
	DateTime.Interval asInterval(Object obj) throws ConfigParseException {
		DateTime.Interval result;
		try {
			result = DateTime.Interval.valueOf(obj.toString().toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new ConfigParseException("Invalid partition: " + obj.toString());
		}
		return result;
	}

	DateTime asDate(JsonNode obj) {
		return dateFactory.getDate(obj);
	}

	DateTimeRange asDateRange(JsonNode obj) {
		DateTime start, end;
		end = dateFactory.getStart();
		if (obj.isArray()) {
			ArrayNode dates = (ArrayNode) obj;
			if (dates.size() < 1 || dates.size() > 2)
				throw new ConfigParseException("Invalid date range: " + obj.toString());
			start = dateFactory.getDate(dates.get(0));
			if (dates.size() > 1)
				end = dateFactory.getDate(dates.get(1));
		} else {
			start = dateFactory.getDate(obj);
		}
		return new DateTimeRange(start, end);
	}
	*/
	
}
