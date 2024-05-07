package sndml.loader;

import java.io.IOException;
import java.sql.SQLException;

import sndml.util.ResourceException;

/**
 * Used for JUnit Tests
 */
public class TestJobRunner extends JobRunner {

	public TestJobRunner(ConnectionProfile profile, JobConfig config) 
			throws ResourceException, SQLException {
		super(profile.newReaderSession(), profile.newDatabaseConnection(), config);
	}
	
	public TestJobRunner(ConnectionProfile profile, YamlFile file) 
			throws IOException, ConfigParseException, ResourceException, SQLException {
		super(profile.newReaderSession(), profile.newDatabaseConnection(), file.getJobConfig(profile));
	}
	

}
