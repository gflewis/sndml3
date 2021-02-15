package sndml.datamart;

import java.io.File;
import java.io.IOException;

/**
 * A file with YAML Loader Config instructions. 
 * This class is used primarily for JUnit tests.
 *
 */
@SuppressWarnings("serial")
public class YamlFile extends File {
		
	public YamlFile(File file) {		
		super(file.getPath());
	}
		
	public LoaderConfig getConfig() 
			throws ConfigParseException, IOException {
		ConfigFactory factory = new ConfigFactory();
		LoaderConfig config = factory.loaderConfig(this, null);
		return config;
	}
	
	public Loader getLoader(ConnectionProfile profile) 
			throws ConfigParseException, IOException {
		LoaderConfig config = getConfig();
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
