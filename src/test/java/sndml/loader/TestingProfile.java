package sndml.loader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestingProfile extends ConnectionProfile {

	private String name;
	
	public TestingProfile(String profileName) throws IOException {
		super(getProfilePath(profileName));
		name = profileName;
	}
	
	/**
	 * Get a file named "sndml_profile" or ".sndml_profile" 
	 * from the directory configs/profileName/
	 * 
	 * Note that the profiles directory is NOT not stored in github 
	 * as it is likely to contain passwords.
	 */
	private static File getProfilePath(String profileName) {
		File file;
		Path directory = Paths.get("configs", profileName);
		file = directory.resolve("sndml_profile").toFile();
		if (file.exists()) return file;
		file = directory.resolve(".sndml_profile").toFile();		
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
