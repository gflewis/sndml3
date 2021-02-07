package sndml.datamart;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import sndml.servicenow.DateTime;
import sndml.servicenow.DateTimeRange;
import sndml.servicenow.Log;
import sndml.servicenow.Table;

public class ConfigFactory {
	
	public DateTimeFactory dateFactory;
	public final ObjectMapper jsonMapper;
	public final ObjectMapper yamlMapper;
	private static Logger logger = LoggerFactory.getLogger(ConfigFactory.class);	
	
	ConfigFactory() {
		this(DateTime.now());
	}
	
	ConfigFactory(DateTime start) {
		this.dateFactory = new DateTimeFactory(start);
		jsonMapper = new ObjectMapper();
		jsonMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
		jsonMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);
		yamlMapper = new ObjectMapper(new YAMLFactory());
		jsonMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);		
		jsonMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);		
	}
	
	void setDateFactory(DateTimeFactory dateFactory) {
		this.dateFactory = dateFactory;
	}
	
	void setMetricsFile(File metricsFile) {
		this.dateFactory.setMetricsFile(metricsFile);
	}
	
	JobConfig jobConfigFromYaml(String yaml) throws ConfigParseException {
		JsonNode node;
		try {
			node = yamlMapper.readTree(yaml);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		return jobConfig(node);
	}

	JobConfig jobConfig(JsonNode node) throws ConfigParseException {
		JobConfig config;
		try {
			config = jsonMapper.treeToValue(node,  JobConfig.class);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		config.setDefaults(this.dateFactory);
		return config;
	}
	
	JobConfig jobConfig(JsonNode node, DateTimeFactory dateFactory) throws ConfigParseException {
		JobConfig config;
		try {
			config = jsonMapper.treeToValue(node,  JobConfig.class);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		config.setDefaults(dateFactory);
		return config;
	}
	
	JobConfig tableLoader(Table table) throws ConfigParseException {
		JobConfig config = new JobConfig();
		config.source = table.getName();
		config.target = table.getName();
		config.setDefaults(dateFactory);
		return config;
	}
		
	
	LoaderConfig loaderConfig(Reader reader, Properties props) throws ConfigParseException {
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
	
}
