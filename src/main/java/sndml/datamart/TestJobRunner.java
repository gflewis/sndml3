package sndml.datamart;

import java.io.IOException;

/**
 * Used for JUnit Tests
 */
public class TestJobRunner extends JobRunner {

	public TestJobRunner(ConnectionProfile profile, JobConfig config) {
		super(profile.getSession(), profile.getDatabase(), config);
	}
	
	public TestJobRunner(ConnectionProfile profile, YamlFile file) throws IOException {
		super(profile.getSession(), profile.getDatabase(), file.getJobConfig(profile));
	}
	

}
