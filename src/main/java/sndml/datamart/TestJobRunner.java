package sndml.datamart;

/**
 * Used for JUnit Tests
 */
public class TestJobRunner extends JobRunner {

	public TestJobRunner(ConnectionProfile profile, JobConfig config) {
		super(profile.getSession(), profile.getDatabase(), config);
	}

}
