package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Used for JUnit Tests
 */
public class TestJobRunner extends JobRunner {

	public TestJobRunner(ConnectionProfile profile, JobConfig config) 
			throws ResourceException, SQLException {
		super(profile.getSession(), profile.getDatabase(), config);
	}
	
	public TestJobRunner(ConnectionProfile profile, YamlFile file) 
			throws IOException, ConfigParseException, ResourceException, SQLException {
		super(profile.getSession(), profile.getDatabase(), file.getJobConfig(profile));
	}
	

}
