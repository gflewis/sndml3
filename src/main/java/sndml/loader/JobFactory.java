package sndml.loader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import sndml.servicenow.Session;
import sndml.util.DateTime;;

public class JobFactory {
	
	final Resources resources;
	final ConnectionProfile profile;
	final Session session;
	final DatabaseWrapper database;	
	final DateCalculator dateCalculator;
	ConfigFactory configFactory = new ConfigFactory();
	
	public JobFactory(Resources resources, DateTime start) {
		this.resources = resources;
		this.profile = resources.getProfile();
		this.session = resources.getReaderSession();
		this.database = resources.getDatabaseWrapper();
		this.dateCalculator = new DateCalculator(start);		
	}
		
	public JobRunner yamlJob(Reader yamlReader) 
			throws ConfigParseException, IOException {
		JobConfig config = configFactory.yamlJob(profile, yamlReader);
		config.initialize(profile, dateCalculator);
		config.validate();
		return new JobRunner(resources, config);
	}
	
	public JobRunner yamlJob(File yamlFile) 
			throws ConfigParseException, IOException {
		return yamlJob(new FileReader(yamlFile));
	}
	
	public JobRunner yamlJob(String yamlText) 
			throws ConfigParseException, IOException {
		return yamlJob(new StringReader(yamlText));
	}

}
