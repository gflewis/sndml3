package sndml.loader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import sndml.servicenow.EncodedQuery;
import sndml.servicenow.RecordKey;
import sndml.servicenow.Table;
import sndml.util.DateTime;
import sndml.util.Log;

public class ConfigFactory {
	
	protected ObjectMapper jsonMapper;
	protected ObjectMapper yamlMapper;
	Logger logger = LoggerFactory.getLogger(ConfigFactory.class);	
	
	public ConfigFactory() {
		this(DateTime.now());
	}
	
	public ConfigFactory(DateTime start) {
		jsonMapper = new ObjectMapper();
		jsonMapper.setSerializationInclusion(Include.NON_NULL);		
	    jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);	    
	    jsonMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
	    jsonMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);
		yamlMapper = new ObjectMapper(new YAMLFactory());
		yamlMapper.setSerializationInclusion(Include.NON_NULL);
	    yamlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
	    yamlMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
	    yamlMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);
	}
	
	YamlLoaderConfig loaderConfig(ConnectionProfile profile, File yamlFile) 
			throws IOException, ConfigParseException {
		Reader reader = new FileReader(yamlFile);
		return loaderConfig(profile, reader);		
	}
		
	YamlLoaderConfig loaderConfig(ConnectionProfile profile, Reader reader) 
			throws IOException, ConfigParseException {
		DateTime start = DateTime.now();
		File metricsFolder = null;
		if (profile != null) {
			String metricsFolderName = profile.loader.getProperty("metrics_folder");
			if (metricsFolderName != null && metricsFolderName.length() > 0)
				metricsFolder = new File(metricsFolderName);
		}
		YamlLoaderConfig loader;
		try {
			loader = yamlMapper.readValue(reader, YamlLoaderConfig.class);			
		}
		catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		logger.info(Log.INIT, "loaderConfig: " + jsonMapper.writeValueAsString(loader));
		loader.setMetricsFolder(metricsFolder);
		File metricsFile = loader.getMetricsFile();
		if (metricsFile != null)
			logger.info(Log.INIT, String.format("metrics=%s", metricsFile));
		DateCalculator dateCalculator = new DateCalculator(start, metricsFile);
		for (JobConfig job : loader.tables) {
			job.initialize(profile, dateCalculator);
			job.validate();
			logger.info(Log.INIT, job.getName() + ": " + job.toString());
		}
		return loader;
	}
		
	JobConfig yamlJob(ConnectionProfile profile, File yamlFile) 
			throws IOException, ConfigParseException {
		return yamlJob(profile, new FileReader(yamlFile));
	}
	
	JobConfig yamlJob(ConnectionProfile profile, String yamlText) 
			throws IOException, ConfigParseException {
		return yamlJob(profile, new StringReader(yamlText));		
	}
	
	JobConfig yamlJob(ConnectionProfile profile, Reader yamlReader) 
			throws IOException, ConfigParseException {
		JobConfig job;
		try {
			job = yamlMapper.readValue(yamlReader, JobConfig.class);			
		}
		catch (JsonProcessingException e1) {
			throw new ConfigParseException(e1);
		}
		return job;
	}
		
	public JobConfig jobConfig(ConnectionProfile profile, JsonNode node) throws ConfigParseException {
		DateCalculator dateFactory = new DateCalculator();
		JobConfig job;
		try {
			job = jsonMapper.treeToValue(node, JobConfig.class);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e.getMessage());
		}
		job.initialize(profile, dateFactory);
		job.validate();
		logger.debug(Log.INIT, "jobConfig: " + job.toString());
		return job;
	}

	JobConfig tableLoader(ConnectionProfile profile, Table table) {
		EncodedQuery query = null;
		return tableLoader(profile, table, query);
	}
	
	JobConfig tableLoader(ConnectionProfile profile, Table table, EncodedQuery query) throws ConfigParseException {
		DateCalculator dateFactory = new DateCalculator();
		JobConfig job = new JobConfig();
		job.source = table.getName();
		if (query != null) job.filter = query.toString();
		job.initialize(profile, dateFactory);
		job.validate();
		logger.info(Log.INIT, "tableLoader: " + job.toString());
		return job;
	}

	JobConfig singleRecordSync(ConnectionProfile profile, Table table, RecordKey docKey) {
		DateCalculator dateFactory = new DateCalculator();
		JobConfig job = new JobConfig();
		job.action = Action.SINGLE;
		job.source = table.getName();
		job.docKey = docKey;
		job.initialize(profile, dateFactory);
		job.validate();
		logger.info(Log.INIT, "singleRecordSync: " + job.toString());
		return job;		
	}

	static void configError(String msg) {
		throw new ConfigParseException(msg);
	}	
		
}
