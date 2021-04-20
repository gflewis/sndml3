package sndml.datamart;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.DateTime;

/**
 * A file with YAML Loader Config instructions. 
 * This class is used primarily for JUnit tests.
 */
@SuppressWarnings("serial")
public class YamlFile extends File {
		
	public YamlFile(File file) {		
		super(file.getPath());
		assert this.exists() : file.getPath() + " does not exist";
		assert this.canRead() : file.getPath() + " is not readable";
		assert hasYamlExt(this) : file.getPath() + " does not have .yaml extension";
	}
	
	public JobConfig getJobConfig(ConnectionProfile profile) 
			throws ConfigParseException, IOException {
		ConfigFactory factory = new ConfigFactory();
		JobConfig config = factory.yamlJob(profile, this);
		return config;		
	}
	
	public JobRunner getJobRunner(ConnectionProfile profile) 
			throws ConfigParseException, IOException, ResourceException, SQLException {
		JobFactory jf = new JobFactory(profile, DateTime.now());
		return jf.yamlJob(this);
	}
	
	public Loader getLoader(ConnectionProfile profile) 
			throws ConfigParseException, IOException, ResourceException, SQLException {
		ConfigFactory factory = new ConfigFactory();
		LoaderConfig config = factory.loaderConfig(null, this);
		return new Loader(profile, config);
	}
	
	/**
	 * Return the name of this file to be displayed when running JUnit tests.
	 */
	@Override	
	public String toString() {
		return this.getName();
	}
	
	/**
	 * Return true if a file has a ".yaml" or ".yml" file extension.
	 */
	public static boolean hasYamlExt(File file) {
		String path = file.getPath();
		return path.endsWith(".yaml") || path.endsWith(".yml");
	}
	
}
