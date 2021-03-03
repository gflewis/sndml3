package sndml.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import sndml.servicenow.Session;

public class JobFactory {

	final ConnectionProfile profile;
	final Session session;
	final Database database;	
	ConfigFactory configFactory = new ConfigFactory();
	DateCalculator dateFactory = new DateCalculator();
	
	public JobFactory(ConnectionProfile profile) {
		this(profile, profile.getDatabase());
	}
	
	public JobFactory(ConnectionProfile profile, Database database) {
		this.profile = profile;
		this.session = profile.getSession();
		this.database = database;		
	}
	
	public JobRunner yamlJob(Reader yamlReader) 
			throws ConfigParseException, IOException {
		JobConfig config = configFactory.yamlJob(profile, yamlReader);
		config.initialize(profile, dateFactory);
		config.validate();
		return new JobRunner(session, database, config);
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
