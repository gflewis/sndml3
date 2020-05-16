package servicenow.api;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import servicenow.datamart.ConnectionProfile;

public class TestingProfile extends ConnectionProfile {

	private String name;
	
	public TestingProfile(String profileName) throws IOException {
		super(getPath(profileName));
		name = profileName;
	}
	
	/**
	 * Get a file named ".sndml_profile" from the directory profiles/profileName/
	 * Note that the profiles directory is NOT not stored in github it they may contain passwords.
	 */
	private static File getPath(String profileName) {
		Path directory = Paths.get("profiles", profileName);
		File file = directory.resolve(".sndml_profile").toFile();
		return file;
	}
	
	public String getName() {
		return this.name;	
	}
	
	@Override
	public String toString() {
		return this.name;
	}
	
}
