package sndml.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;

import sndml.servicenow.DateTime;
import sndml.servicenow.Session;

public class JobFactory {

	final ConnectionProfile profile;
	final Session session;
	final Database database;	
	final DateCalculator dateCalculator;
	ConfigFactory configFactory = new ConfigFactory();
	
	public JobFactory(ConnectionProfile profile, DateTime start)
			throws ResourceException, SQLException {
		this(profile, profile.getSession(), profile.getDatabase(),start);
	}
	
	public JobFactory(ConnectionProfile profile, Session session, Database database, DateTime start) {
		this.profile = profile;
		this.session = session;
		this.database = database;		
		dateCalculator = new DateCalculator(start);
	}
	
	public JobRunner yamlJob(Reader yamlReader) 
			throws ConfigParseException, IOException {
		JobConfig config = configFactory.yamlJob(profile, yamlReader);
		config.initialize(profile, dateCalculator);
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
