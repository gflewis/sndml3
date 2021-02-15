package sndml.datamart;

import java.io.File;
import java.io.FilenameFilter;

import sndml.datamart.YamlFile;

/**
 * A folder in src/test/resources which contains files for JUnit tests.
 *
 */
@SuppressWarnings("serial")
public class TestFolder extends File {

	FilenameFilter yamlFilter = (dir, name) -> name.endsWith(".yaml");			
	
	public TestFolder(String pathname) {
		super("src/test/resources/" + pathname);
	}
	
	/**
	 * Get a file from this folder
	 */
	public File getFile(String name) {
		return new File(this, name);
	}
	
	public YamlFile getYaml(String name) {
		return new YamlFile(new File(this, name + ".yaml"));
	}
	
	public YamlFile[] yamlFiles() {
		File[] files = listFiles(yamlFilter);
		YamlFile[] result = new YamlFile[files.length];
		for (int i = 0; i < files.length; ++i) result[i] = new YamlFile(files[i]);
		return result;
	}

}
