package sndml.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class JobFactory {

	ConfigFactory configFactory = new ConfigFactory();
	DateCalculator dateFactory = new DateCalculator();
		
	public JobRunner yamlJob(ConnectionProfile profile, Reader yamlReader) 
			throws ConfigParseException, IOException {
		JobConfig config = configFactory.yamlJob(profile, yamlReader);
		config.initialize(profile, dateFactory);
		config.validate();
		return new JobConfigRunner(profile, config);
	}
	
	public JobRunner yamlJob(ConnectionProfile profile, File yamlFile) 
			throws ConfigParseException, IOException {
		return yamlJob(profile, new FileReader(yamlFile));
	}
	
	public JobRunner yamlJob(ConnectionProfile profile, String yamlText) 
			throws ConfigParseException, IOException {
		return yamlJob(profile, new StringReader(yamlText));
	}

}
