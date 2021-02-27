package sndml.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

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
	
	LoaderConfig loaderConfig(ConnectionProfile profile, File yamlFile) 
			throws IOException, ConfigParseException {
		Reader reader = new FileReader(yamlFile);
		return loaderConfig(profile, reader);		
	}
		
	LoaderConfig loaderConfig(ConnectionProfile profile, Reader reader) 
			throws IOException, ConfigParseException {
		File metricsFolder = null;			
		if (profile != null) {
			String metricsFolderName = profile.getProperty("loader.metrics_folder");
			if (metricsFolderName != null && metricsFolderName.length() > 0)
				metricsFolder = new File(metricsFolderName);
		}
		LoaderConfig loader;
		try {
			loader = yamlMapper.readValue(reader, LoaderConfig.class);			
		}
		catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		loader.setMetricsFolder(metricsFolder);
		DateCalculator dateFactory = new DateCalculator();
		// logger.info(Log.INIT, "loaderConfig last=" + dateFactory.getLastStart());
		logger.info(Log.INIT, "loaderConfig: " + jsonMapper.writeValueAsString(loader));
		for (JobConfig job : loader.tables) {
			job.initialize(profile, dateFactory);
			job.validate();
			logger.info(Log.INIT, job.getName() + ": " + job.toString());
		}
		return loader;
	}
	
//	ScannerInput scannerConfig(ConnectionProfile profile, Reader reader) 
//			throws IOException, ConfigParseException {
//		ScannerInput config = null;
//		try {
//			config = jsonMapper.readValue(reader,  ScannerInput.class);
//		} catch (InvalidFormatException e) {
//			throw new ConfigParseException(e);
//		}
//		return config;
//	}
	
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
		
	JobConfig jobConfig(ConnectionProfile profile, JsonNode node) throws ConfigParseException {
		DateCalculator dateFactory = new DateCalculator();
		JobConfig job;
		try {
			job = jsonMapper.treeToValue(node, JobConfig.class);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e.getMessage());
		}
		job.initialize(profile, dateFactory);
		job.validate();
		logger.info(Log.INIT, "jobConfig: " + job.toString());
		return job;
	}
		
	JobConfig tableLoader(ConnectionProfile profile, Table table) throws ConfigParseException {
		DateCalculator dateFactory = new DateCalculator();
		JobConfig job = new JobConfig();
		job.source = table.getName();
		job.initialize(profile, dateFactory);
		job.validate();
		logger.info(Log.INIT, "tableLoader: " + job.toString());
		return job;
	}


	static void configError(String msg) {
		throw new ConfigParseException(msg);
	}	
		
}
